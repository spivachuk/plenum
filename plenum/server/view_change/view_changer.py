from collections import deque
from typing import List, Optional, Tuple

from stp_core.common.log import getlogger
from stp_core.ratchet import Ratchet

from plenum.common.throttler import Throttler
from plenum.common.constants import PRIMARY_SELECTION_PREFIX, \
    VIEW_CHANGE_PREFIX, MONITORING_PREFIX, POOL_LEDGER_ID
from plenum.common.messages.node_messages import InstanceChange, ViewChangeDone
from plenum.common.util import mostCommonElement, SortedDict
from plenum.common.message_processor import MessageProcessor
from plenum.server.models import InstanceChanges
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.suspicion_codes import Suspicions
from plenum.server.router import Router


logger = getlogger()

# TODO docs and types
# TODO logging


class ViewChanger(HasActionQueue, MessageProcessor):

    def __init__(self, node):
        self.node = node

        self.view_no = 0  # type: int

        HasActionQueue.__init__(self)

        self.inBox = deque()
        self.outBox = deque()
        self.inBoxRouter = Router(
            (InstanceChange, self.process_instance_change_msg),
            (ViewChangeDone, self.process_vchd_msg)
        )

        self.instanceChanges = InstanceChanges()

        # The quorum of `ViewChangeDone` msgs is different depending on whether we're doing a real view change,
        # or just propagating view_no and Primary from `CurrentState` messages sent to a newly joined Node.
        # TODO: separate real view change and Propagation of Primary
        # TODO: separate catch-up, view-change and primary selection so that
        # they are really independent.
        self.propagate_primary = False

        # Tracks if other nodes are indicating that this node is in lower view
        # than others. Keeps a map of view no to senders
        # TODO: Consider if sufficient ViewChangeDone for 2 different (and
        # higher views) are received, should one view change be interrupted in
        # between.
        self._next_view_indications = SortedDict()

        self._view_change_in_progress = False

        self.previous_master_primary = None

        self.set_defaults()

        self.initInsChngThrottling()

    def __repr__(self):
        return "{}".format(self.name)

    # PROPERTIES

    @property
    def name(self):
        return self.node.name

    @property
    def config(self):
        return self.node.config

    @property
    def quorums(self):
        return self.node.quorums

    @property
    def view_change_in_progress(self):
        return self._view_change_in_progress

    @view_change_in_progress.setter
    def view_change_in_progress(self, value):
        self._view_change_in_progress = value

    @property
    def quorum(self) -> int:
        # TODO: re-factor this, separate this two states (selection of a new
        # primary and propagation of existing one)
        if not self.view_change_in_progress:
            return self.node.quorums.propagate_primary.value
        if self.propagate_primary:
            return self.node.quorums.propagate_primary.value
        return self.node.quorums.view_change_done.value

    @property
    def _hasViewChangeQuorum(self):
        # This method should just be present for master instance.
        """
        Checks whether n-f nodes completed view change and whether one
        of them is the next primary
        """
        num_of_ready_nodes = len(self._view_change_done)
        diff = self.quorum - num_of_ready_nodes
        if diff > 0:
            logger.debug(
                '{} needs {} ViewChangeDone messages'.format(self, diff))
            return False

        logger.debug("{} got view change quorum ({} >= {})"
                     .format(self.name,
                             num_of_ready_nodes,
                             self.quorum))
        return True

    @property
    def _is_propagated_view_change_completed(self):
        if not self._propagated_view_change_completed and \
                self.node.poolLedger is not None and \
                self.propagate_primary:

            accepted = self.get_sufficient_same_view_change_done_messages()
            if accepted is not None:
                accepted_pool_ledger_i = \
                    next(filter(lambda x: x[0] == POOL_LEDGER_ID,
                                accepted[1]))
                self_pool_ledger_i = \
                    next(filter(lambda x: x[0] == POOL_LEDGER_ID,
                                self.node.ledger_summary))
                logger.debug("{} Primary selection has been already completed "
                             "on pool ledger info = {}, primary {}, self pool "
                             "ledger info {}".format(
                                 self, accepted_pool_ledger_i,
                                 accepted[0],
                                 self_pool_ledger_i))
                self._propagated_view_change_completed = True

        return self._propagated_view_change_completed

    @property
    def has_view_change_from_primary(self) -> bool:
        if not self._has_view_change_from_primary:
            next_primary_name = self.node.elector.next_primary_node_name(0)

            if next_primary_name not in self._view_change_done:
                logger.debug(
                    "{} has not received ViewChangeDone from the next "
                    "primary {} (view_no: {}, totalNodes: {})". format(
                        self.name, next_primary_name,
                        self.view_no, self.node.totalNodes))
            else:
                logger.debug('{} received ViewChangeDone from primary {}'
                             .format(self, next_primary_name))
                self._has_view_change_from_primary = True

        return self._has_view_change_from_primary

    @property
    def has_acceptable_view_change_quorum(self):
        if not self._has_acceptable_view_change_quorum:
            self._has_acceptable_view_change_quorum = (
                self._hasViewChangeQuorum and
                (self._is_propagated_view_change_completed or
                 self.has_view_change_from_primary)
            )
        return self._has_acceptable_view_change_quorum

    @property
    def is_behind_for_view(self) -> bool:
        # Checks if the node is currently behind the accepted state for this
        # view, only makes sense to call when the node has an acceptable
        # view change quorum
        _, accepted_ledger_summary = self.get_sufficient_same_view_change_done_messages()
        for (ledgerId, own_ledger_size, _), (_, accepted_ledger_size, _) in \
                zip(self.node.ledger_summary, accepted_ledger_summary):
            if own_ledger_size < accepted_ledger_size:
                logger.debug("{} ledger {} sizes are differ: own {} accepted {}"
                             "".format(self, ledgerId, own_ledger_size, accepted_ledger_size))
                return True
        return False

    # __ PROPERTIES __

    # EXTERNAL EVENTS

    def on_master_degradation(self):
        """
        """
        view_no = self.view_no + 1
        logger.info(
            "{} sending instance with view_no = {} and trying to start "
            "view change since performance of master instance degraded"
            "".format(self, view_no))
        self.sendInstanceChange(view_no)
        self.do_view_change_if_possible(view_no)

    def on_primary_loss(self):
        view_no = self.view_no + 1
        logger.info("{} sending instance with view_no = {} and trying "
                    "to start view change since primary was lost"
                    "".format(self, view_no))
        self.sendInstanceChange(view_no,
                                Suspicions.PRIMARY_DISCONNECTED)
        self.do_view_change_if_possible(view_no)

    # TODO we have `on_primary_loss`, do we need that one?
    def on_primary_about_to_be_disconnected(self):
        view_no = self.view_no + 1
        logger.info("{} sending instance with view_no = {} "
                    "since primary is about to be disconnected"
                    "".format(self, view_no))
        self.sendInstanceChange(
            view_no, Suspicions.PRIMARY_ABOUT_TO_BE_DISCONNECTED)

    def on_suspicious_primary(self, suspicion: Suspicions):
        view_no = self.view_no + 1
        logger.info("{} sending instance with view_no = {} since primary "
                    "seems suspicious, reason {}"
                    "".format(self, view_no, suspicion.reason))
        self.sendInstanceChange(view_no, suspicion)
        # TODO why we don't try to start view change here

    def on_view_change_not_completed_in_time(self):
        view_no = self.view_no + 1
        logger.info("{} sending instance with view_no = {} since "
                    "view change to view {} is not completed in time"
                    "".format(self, view_no, self.view_no))
        self.sendInstanceChange(view_no,
                                Suspicions.INSTANCE_CHANGE_TIMEOUT)

    def on_catchup_complete(self):
        if self.node.is_synced and self.node.master_replica.isPrimary is None and \
                not self._is_propagated_view_change_completed:
            self._send_view_change_done_message()

        self._start_selection()

    def on_future_view_vchd_msg(self, view_no, frm):
        assert view_no > self.view_no
        if view_no not in self._next_view_indications:
            self._next_view_indications[view_no] = set()
        self._next_view_indications[view_no].add(frm)
        self._start_view_change_if_possible(view_no)

    # __ EXTERNAL EVENTS __

    def process_instance_change_msg(self, instChg: InstanceChange, frm: str) -> None:
        """
        Validate and process an instance change request.

        :param instChg: the instance change request
        :param frm: the name of the node that sent this `msg`
        """
        logger.debug("{} received instance change request: {} from {}".
                     format(self, instChg, frm))

        # TODO: add sender to blacklist?
        if not isinstance(instChg.viewNo, int):
            self.node.discard(
                instChg, "{}field view_no has incorrect type: {}".
                format(VIEW_CHANGE_PREFIX, type(instChg.viewNo)))
        elif instChg.viewNo <= self.view_no:
            self.node.discard(
                instChg,
                "Received instance change request with view no {} "
                "which is not more than its view no {}".
                format(instChg.viewNo, self.view_no), logger.info)
        else:
            # Record instance changes for views but send instance change
            # only when found master to be degraded. if quorum of view changes
            #  found then change view even if master not degraded
            self._on_verified_instance_change_msg(instChg, frm)

            if self.instanceChanges.hasInstChngFrom(instChg.viewNo, self.name):
                logger.debug(
                    "{} received instance change message {} but has already "
                    "sent an instance change message".format(self, instChg))
            elif not self.node.monitor.isMasterDegraded():
                logger.debug(
                    "{} received instance change message {} but did not "
                    "find the master to be slow".format(self, instChg))
            else:
                logger.info(
                    "{}{} found master degraded after receiving instance change"
                    " message from {}".format(
                        VIEW_CHANGE_PREFIX, self, frm))
                self.sendInstanceChange(instChg.viewNo)

    def process_vchd_msg(self, msg: ViewChangeDone, sender: str) -> bool:
        """
        Processes ViewChangeDone messages. Once n-f messages have been
        received, decides on a primary for specific replica.

        :param msg: ViewChangeDone message
        :param sender: the name of the node from which this message was sent
        """

        logger.debug("{}'s primary selector started processing of "
                     "ViewChangeDone msg from {} : {}"
                     .format(self.name, sender, msg))

        view_no = msg.viewNo

        if self.view_no != view_no:
            self.discard(
                msg,
                '{} got Primary from {} for view no {} '
                'whereas current view no is {}'
                .format(self, sender, view_no, self.view_no),
                logMethod=logger.debug)
            return False

        new_primary_name = msg.name
        if new_primary_name == self.previous_master_primary:
            self.discard(
                msg,
                '{} got Primary from {} for {} who was primary of '
                'master in previous view too'
                .format(self, sender, new_primary_name),
                logMethod=logger.debug)
            return False

        # Since a node can send ViewChangeDone more than one time
        self._on_verified_view_change_done_msg(msg, sender)
        # TODO why do we check that after the message tracking
        if self.node.master_replica.hasPrimary:
            self.discard(
                msg,
                "it already decided primary which is {}".
                format(self.node.master_replica.primaryName),
                logger.debug)
            return False

        self._start_selection()

    def send(self, msg):
        """
        Send a message to the node.

        :param msg: the message to send
        """
        logger.debug("{}'s view_changer sending {}".format(self.name, msg))
        self.outBox.append(msg)

    async def serviceQueues(self, limit=None) -> int:
        """
        Service at most `limit` messages from the inBox.

        :param limit: the maximum number of messages to service
        :return: the number of messages successfully processed
        """

        return await self.inBoxRouter.handleAll(self.inBox, limit)

    def sendInstanceChange(self, view_no: int,
                           suspicion=Suspicions.PRIMARY_DEGRADED):
        """
        Broadcast an instance change request to all the remaining nodes

        :param view_no: the view number when the instance change is requested
        """

        # If not found any sent instance change messages in last
        # `ViewChangeWindowSize` seconds or the last sent instance change
        # message was sent long enough ago then instance change message can be
        # sent otherwise no.
        canSendInsChange, cooldown = self.insChngThrottler.acquire()
        if canSendInsChange:
            logger.info(
                "{}{} sending an instance change with view_no {}"
                " since {}".format(
                    VIEW_CHANGE_PREFIX,
                    self,
                    view_no,
                    suspicion.reason))
            logger.info("{}{} metrics for monitor: {}"
                        .format(MONITORING_PREFIX, self,
                                self.node.monitor.prettymetrics))
            msg = self._create_instance_change_msg(view_no, suspicion.code)
            self.send(msg)
            # record instance change vote for self and try to change the view
            # if quorum is reached
            self._on_verified_instance_change_msg(msg, self.name)
        else:
            logger.debug(
                "{} cannot send instance change sooner then {} seconds".format(
                    self, cooldown))

    # noinspection PyAttributeOutsideInit
    def initInsChngThrottling(self):
        windowSize = self.node.config.ViewChangeWindowSize
        ratchet = Ratchet(a=2, b=0.05, c=1, base=2, peak=windowSize)
        self.insChngThrottler = Throttler(windowSize, ratchet.get)

    def _create_instance_change_msg(self, view_no, suspicion_code):
        return InstanceChange(view_no, suspicion_code)

    def _on_verified_instance_change_msg(self, msg, frm):
        view_no = msg.viewNo

        if not self.instanceChanges.hasInstChngFrom(view_no, frm):
            self.instanceChanges.addVote(msg, frm)
            if view_no > self.view_no:
                self.do_view_change_if_possible(view_no)

    def do_view_change_if_possible(self, view_no):
        # TODO: Need to handle skewed distributions which can arise due to
        # malicious nodes sending messages early on
        can, whyNot = self._canViewChange(view_no)
        if can:
            logger.info("{}{} initiating a view change to {} from {}".
                        format(VIEW_CHANGE_PREFIX, self, view_no, self.view_no))
            self.propagate_primary = False
            self.startViewChange(view_no)
        else:
            logger.debug(whyNot)
        return can

    def _start_view_change_if_possible(self, view_no) -> bool:
        ind_count = len(self._next_view_indications[view_no])
        if self.quorums.propagate_primary.is_reached(ind_count):
            logger.info(
                '{}{} starting view change for {} after {} view change '
                'indications from other nodes'.format(
                    VIEW_CHANGE_PREFIX, self, view_no, ind_count))
            self.propagate_primary = True
            self.startViewChange(view_no)
            return True
        return False

    def _canViewChange(self, proposedViewNo: int) -> (bool, str):
        """
        Return whether there's quorum for view change for the proposed view
        number and its view is less than or equal to the proposed view
        """
        msg = None
        quorum = self.quorums.view_change.value
        if not self.instanceChanges.hasQuorum(proposedViewNo, quorum):
            msg = '{} has no quorum for view {}'.format(self, proposedViewNo)
        elif not proposedViewNo > self.view_no:
            msg = '{} is in higher view more than {}'.format(
                self, proposedViewNo)

        return not bool(msg), msg

    def startViewChange(self, proposed_view_no: int):
        """
        Trigger the view change process.

        :param proposed_view_no: the new view number after view change.
        """
        # TODO: consider moving this to pool manager
        # TODO: view change is a special case, which can have different
        # implementations - we need to make this logic pluggable

        for view_no in tuple(self._next_view_indications.keys()):
            if view_no > proposed_view_no:
                break
            self._next_view_indications.pop(view_no)

        self.view_no = proposed_view_no
        self.view_change_in_progress = True
        self.previous_master_primary = self.node.master_primary_name
        self.set_defaults()
        self.initInsChngThrottling()

        self.node.on_view_change_start()
        self.node.start_catchup()

    def _on_verified_view_change_done_msg(self, msg, frm):
        new_primary_name = msg.name
        ledger_summary = msg.ledgerInfo

        # TODO what is the case when node sends several different
        # view change done messages
        data = (new_primary_name, ledger_summary)
        self._view_change_done[frm] = data

    def _start_selection(self):

        error = None

        if not self.node.is_synced:
            error = "mode is {}".format(self.node.mode)
        elif not self.has_acceptable_view_change_quorum:
            error = "has no view change quorum or no message from next primary"
        else:
            rv = self.get_sufficient_same_view_change_done_messages()
            if rv is None:
                error = "there are not sufficient same ViewChangeDone messages"
            elif not (self._is_propagated_view_change_completed or
                      self._verify_primary(*rv)):
                error = "failed to verify primary"

        if error is not None:
            logger.debug('{} cannot start primary selection because {}'
                         .format(self, error))
            return

        if self.is_behind_for_view:
            logger.debug(
                '{} is synced and has an acceptable view change quorum '
                'but is behind the accepted state'.format(self))
            self.node.start_catchup()
            return

        logger.debug("{} starting selection".format(self))

        nodeReg = None
        # in case of already completed view change
        # use node registry actual for the moment when it happened
        if self._is_propagated_view_change_completed:
            assert self._accepted_view_change_done_message is not None
            ledger_summary = self._accepted_view_change_done_message[1]
            pool_ledger_size = ledger_summary[POOL_LEDGER_ID][1]
            nodeReg = self.node.poolManager.getNodeRegistry(pool_ledger_size)

        self.node.select_primaries(nodeReg)

        if self.view_change_in_progress:
            self.view_change_in_progress = False
            self.node.on_view_change_complete()
            # when we had INSTANCE_CHANGE message, they added into instanceChanges
            # by msg.view_no. When view change was occured and view_no is changed,
            # then we should delete all INSTANCE_CHANGE messages with current (already changed)
            # view_no (which used in corresponded INSTANCE_CHANGE messages)
            # Therefore we delete all INSTANCE_CHANGE messages from previous and current view number
            for view_number in list(self.instanceChanges.keys()):
                if view_number <= self.view_no:
                    self.instanceChanges.pop(view_number, None)
            self.previous_master_primary = None
            self.propagate_primary = False

    def set_defaults(self):
        # Tracks view change done message
        self._view_change_done = {}  # replica name -> data

        # Set when an appropriate view change quorum is found which has
        # sufficient same ViewChangeDone messages
        self._primary_verified = False

        self._has_view_change_from_primary = False

        self._has_acceptable_view_change_quorum = False

        self._accepted_view_change_done_message = None

        # accept any primary if propagating view change done msgs
        # for already completed view change
        self._propagated_view_change_completed = False

    def get_sufficient_same_view_change_done_messages(self) -> Optional[Tuple]:
        # Returns whether has a quorum of ViewChangeDone messages that are same
        # TODO: Does not look like optimal implementation.
        if self._accepted_view_change_done_message is None and \
                self._view_change_done:
            votes = self._view_change_done.values()
            votes = [(nm, tuple(tuple(i) for i in info)) for nm, info in votes]
            (new_primary, ledger_info), vote_count = mostCommonElement(votes)
            if vote_count >= self.quorum:
                logger.debug(
                    '{} found acceptable primary {} and ledger info {}'. format(
                        self, new_primary, ledger_info))
                self._accepted_view_change_done_message = (new_primary,
                                                           ledger_info)
            else:
                logger.debug('{} does not have acceptable primary, only {} '
                             'votes for {}'.format(self, vote_count,
                                                   (new_primary, ledger_info)))

        return self._accepted_view_change_done_message

    def _verify_primary(self, new_primary, ledger_info):
        """
        This method is called when sufficient number of ViewChangeDone
        received and makes steps to switch to the new primary
        """
        expected_primary = self.node.elector.next_primary_node_name(0)
        if new_primary != expected_primary:
            logger.error("{}{} expected next primary to be {}, but majority "
                         "declared {} instead for view {}"
                         .format(PRIMARY_SELECTION_PREFIX, self.name,
                                 expected_primary, new_primary, self.view_no))
            return False

        self._primary_verified = True
        return True
        # TODO: check if ledger status is expected

    def _send_view_change_done_message(self):
        """
        Sends ViewChangeDone message to other protocol participants
        """
        new_primary_name = self.node.elector.next_primary_node_name(0)
        ledger_summary = self.node.ledger_summary
        message = ViewChangeDone(self.view_no,
                                 new_primary_name,
                                 ledger_summary)

        logger.debug("{} is sending ViewChangeDone msg to all : {}"
                     "".format(self, message))

        self.send(message)
        self._on_verified_view_change_done_msg(message, self.name)

    # overridden method of PrimaryDecider
    def get_msgs_for_lagged_nodes(self) -> List[ViewChangeDone]:
        # Should not return a list, only done for compatibility with interface
        """
        Returns the last accepted `ViewChangeDone` message.
        If no view change has happened returns ViewChangeDone
        with view no 0 to a newly joined node
        """
        # TODO: Consider a case where more than one node joins immediately,
        # then one of the node might not have an accepted
        # ViewChangeDone message
        messages = []
        accepted = self._accepted_view_change_done_message
        if accepted:
            messages.append(ViewChangeDone(self.view_no, *accepted))
        elif self.name in self._view_change_done:
            messages.append(ViewChangeDone(self.view_no,
                                           *self._view_change_done[self.name]))
        else:
            logger.debug(
                '{} has no ViewChangeDone message to send for view {}'. format(
                    self, self.view_no))
        return messages
