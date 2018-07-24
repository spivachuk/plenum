import time
from collections import deque, OrderedDict
from enum import unique, IntEnum
from hashlib import sha256
from typing import List, Dict, Optional, Any, Set, Tuple, Callable

import math

import sys

from common.exceptions import LogicError, PlenumValueError
from common.serializers.serialization import serialize_msg_for_signing, state_roots_serializer
from crypto.bls.bls_bft_replica import BlsBftReplica
from orderedset import OrderedSet

from plenum.common.config_util import getConfig
from plenum.common.constants import THREE_PC_PREFIX, PREPREPARE, PREPARE, \
    ReplicaHooks, DOMAIN_LEDGER_ID, COMMIT
from plenum.common.exceptions import SuspiciousNode, \
    InvalidClientMessageException, UnknownIdentifier
from plenum.common.hook_manager import HookManager
from plenum.common.message_processor import MessageProcessor
from plenum.common.messages.message_base import MessageBase
from plenum.common.messages.node_messages import Reject, Ordered, \
    PrePrepare, Prepare, Commit, Checkpoint, ThreePCState, CheckpointState, ThreePhaseMsg, ThreePhaseKey
from plenum.common.request import Request, ReqKey
from plenum.common.types import f
from plenum.common.util import updateNamedTuple, compare_3PC_keys, max_3PC_key, \
    mostCommonElement, SortedDict, firstKey
from plenum.config import CHK_FREQ
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.models import Commits, Prepares
from plenum.server.router import Router
from plenum.server.suspicion_codes import Suspicions
from sortedcontainers import SortedList
from stp_core.common.log import getlogger

import plenum.server.node

LOG_TAGS = {
    'PREPREPARE': {"tags": ["node-preprepare"]},
    'PREPARE': {"tags": ["node-prepare"]},
    'COMMIT': {"tags": ["node-commit"]},
    'ORDERED': {"tags": ["node-ordered"]}
}


@unique
class TPCStat(IntEnum):  # TPC => Three-Phase Commit
    ReqDigestRcvd = 0
    PrePrepareSent = 1
    PrePrepareRcvd = 2
    PrepareRcvd = 3
    PrepareSent = 4
    CommitRcvd = 5
    CommitSent = 6
    OrderSent = 7


class Stats:
    def __init__(self, keys):
        sort = sorted([k.value for k in keys])
        self.stats = OrderedDict((s, 0) for s in sort)

    def inc(self, key):
        """
        Increment the stat specified by key.
        """
        self.stats[key] += 1

    def get(self, key):
        return self.stats[key]

    def __repr__(self):
        return str({TPCStat(k).name: v for k, v in self.stats.items()})


class Replica3PRouter(Router):
    def __init__(self, replica, *args, **kwargs):
        self.replica = replica
        super().__init__(*args, *kwargs)

    # noinspection PyCallingNonCallable
    def handleSync(self, msg: Any) -> Any:
        try:
            super().handleSync(msg)
        except SuspiciousNode as ex:
            self.replica.node.reportSuspiciousNodeEx(ex)


PP_CHECK_NOT_FROM_PRIMARY = 0
PP_CHECK_TO_PRIMARY = 1
PP_CHECK_DUPLICATE = 2
PP_CHECK_OLD = 3
PP_CHECK_REQUEST_NOT_FINALIZED = 4
PP_CHECK_NOT_NEXT = 5
PP_CHECK_WRONG_TIME = 6

PP_APPLY_REJECT_WRONG = 7
PP_APPLY_WRONG_DIGEST = 8
PP_APPLY_WRONG_STATE = 9
PP_APPLY_ROOT_HASH_MISMATCH = 10
PP_APPLY_HOOK_ERROR = 11


class Replica(HasActionQueue, MessageProcessor, HookManager):
    STASHED_CHECKPOINTS_BEFORE_CATCHUP = 1
    HAS_NO_PRIMARY_WARN_THRESCHOLD = 10

    def __init__(self, node: 'plenum.server.node.Node', instId: int,
                 config=None,
                 isMaster: bool = False,
                 bls_bft_replica: BlsBftReplica = None):
        """
        Create a new replica.

        :param node: Node on which this replica is located
        :param instId: the id of the protocol instance the replica belongs to
        :param isMaster: is this a replica of the master protocol instance
        """
        HasActionQueue.__init__(self)
        self.stats = Stats(TPCStat)
        self.config = config or getConfig()

        self.inBoxRouter = Router(
            (ReqKey, self.readyFor3PC),
            (PrePrepare, self.processThreePhaseMsg),
            (Prepare, self.processThreePhaseMsg),
            (Commit, self.processThreePhaseMsg),
            (Checkpoint, self.processCheckpoint),
            (ThreePCState, self.process3PhaseState),
        )

        self.threePhaseRouter = Replica3PRouter(
            self,
            (PrePrepare, self.processPrePrepare),
            (Prepare, self.processPrepare),
            (Commit, self.processCommit)
        )

        self.node = node
        self.instId = instId
        self.name = self.generateName(node.name, self.instId)
        self.logger = getlogger(self.name)

        self.outBox = deque()
        """
        This queue is used by the replica to send messages to its node. Replica
        puts messages that are consumed by its node
        """

        self.inBox = deque()
        """
        This queue is used by the replica to receive messages from its node.
        Node puts messages that are consumed by the replica
        """

        self.inBoxStash = deque()
        """
        If messages need to go back on the queue, they go here temporarily and
        are put back on the queue on a state change
        """

        self.isMaster = isMaster

        # Indicates name of the primary replica of this protocol instance.
        # None in case the replica does not know who the primary of the
        # instance is
        self._primaryName = None  # type: Optional[str]

        # TODO: Rename since it will contain all messages till primary is
        # selected, primary selection is only done once pool ledger is
        # caught up
        # Requests waiting to be processed once the replica is able to decide
        # whether it is primary or not
        self.postElectionMsgs = deque()

        # PRE-PREPAREs that are waiting to be processed but do not have the
        # corresponding request finalised. Happens when replica has not been
        # forwarded the request by the node but is getting 3 phase messages.
        # The value is a list since a malicious entry might send PRE-PREPARE
        # with a different digest and since we dont have the request finalised
        # yet, we store all PRE-PPREPAREs
        # type: List[Tuple[PrePrepare, str, Set[Tuple[str, int]]]]
        self.prePreparesPendingFinReqs = []

        # PrePrepares waiting for previous PrePrepares, key being tuple of view
        # number and pre-prepare sequence numbers and value being tuple of
        # PrePrepare and sender
        # TODO: Since pp_seq_no will start from 1 in each view, the comparator
        # of SortedDict needs to change
        self.prePreparesPendingPrevPP = SortedDict(lambda k: (k[0], k[1]))

        # PREPAREs that are stored by non primary replica for which it has not
        #  got any PRE-PREPARE. Dictionary that stores a tuple of view no and
        #  prepare sequence number as key and a deque of PREPAREs as value.
        # This deque is attempted to be flushed on receiving every
        # PRE-PREPARE request.
        self.preparesWaitingForPrePrepare = {}
        # type: Dict[Tuple[int, int], deque]

        # COMMITs that are stored for which there are no PRE-PREPARE or PREPARE
        # received
        self.commitsWaitingForPrepare = {}
        # type: Dict[Tuple[int, int], deque]

        # Dictionary of sent PRE-PREPARE that are stored by primary replica
        # which it has broadcasted to all other non primary replicas
        # Key of dictionary is a 2 element tuple with elements viewNo,
        # pre-prepare seqNo and value is the received PRE-PREPARE
        self.sentPrePrepares = SortedDict(lambda k: (k[0], k[1]))
        # type: Dict[Tuple[int, int], PrePrepare]

        # Dictionary of received PRE-PREPAREs. Key of dictionary is a 2
        # element tuple with elements viewNo, pre-prepare seqNo and value
        # is the received PRE-PREPARE
        self.prePrepares = SortedDict(lambda k: (k[0], k[1]))
        # type: Dict[Tuple[int, int], PrePrepare]

        # Dictionary of received Prepare requests. Key of dictionary is a 2
        # element tuple with elements viewNo, seqNo and value is a 2 element
        # tuple containing request digest and set of sender node names(sender
        # replica names in case of multiple protocol instances)
        # (viewNo, seqNo) -> ((identifier, reqId), {senders})
        self.prepares = Prepares()
        # type: Dict[Tuple[int, int], Tuple[Tuple[str, int], Set[str]]]

        self.commits = Commits()
        # type: Dict[Tuple[int, int], Tuple[Tuple[str, int], Set[str]]]

        # Set of tuples to keep track of ordered requests. Each tuple is
        # (viewNo, ppSeqNo).
        self.ordered = OrderedSet()  # type: OrderedSet[Tuple[int, int]]

        # Dictionary to keep track of the which replica was primary during each
        # view. Key is the view no and value is the name of the primary
        # replica during that view
        self.primaryNames = OrderedDict()  # type: OrderedDict[int, str]

        # Commits which are not being ordered since commits with lower
        # sequence numbers have not been ordered yet. Key is the
        # viewNo and value a map of pre-prepare sequence number to commit
        # type: Dict[int,Dict[int,Commit]]
        self.stashed_out_of_order_commits = {}

        self.checkpoints = SortedDict(lambda k: k[1])

        # Stashed checkpoints for each view. The key of the outermost
        # dictionary is the view_no, value being a dictionary with key as the
        # range of the checkpoint and its value again being a mapping between
        # senders and their sent checkpoint
        self.stashedRecvdCheckpoints = {}  # type: Dict[int, Dict[Tuple,
        # Dict[str, Checkpoint]]]

        self.stashingWhileOutsideWaterMarks = deque()

        # Flag being used for preterm exit from the loop in the method
        # `processStashedMsgsForNewWaterMarks`. See that method for details.
        self.consumedAllStashedMsgs = True

        # Low water mark
        self._h = 0  # type: int
        # Set high water mark (`H`) too
        self.h = 0  # type: int

        self._lastPrePrepareSeqNo = self.h  # type: int

        # Queues used in PRE-PREPARE for each ledger,
        self.requestQueues = {}  # type: Dict[int, OrderedSet]
        for ledger_id in self.ledger_ids:
            self.register_ledger(ledger_id)

        self.batches = OrderedDict()  # type: OrderedDict[Tuple[int, int],
        # Tuple[int, float, bytes]]

        # TODO: Need to have a timer for each ledger
        self.lastBatchCreated = time.perf_counter()

        # self.lastOrderedPPSeqNo = 0
        # Three phase key for the last ordered batch
        self._last_ordered_3pc = (0, 0)

        # 3 phase key for the last prepared certificate before view change
        # started, applicable only to master instance
        self.last_prepared_before_view_change = None

        # Tracks for which keys PRE-PREPAREs have been requested.
        # Cleared in `gc`
        # type: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self.requested_pre_prepares = {}

        # Tracks for which keys PREPAREs have been requested.
        # Cleared in `gc`
        # type: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self.requested_prepares = {}

        # Tracks for which keys COMMITs have been requested.
        # Cleared in `gc`
        # type: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]]
        self.requested_commits = {}

        # Time of the last PRE-PREPARE which satisfied all validation rules
        # (time, digest, roots were all correct). This time is not to be
        # reverted even if the PRE-PREPAREs are not ordered. This implies that
        # the next primary would have seen all accepted PRE-PREPAREs or another
        # view change will happen
        self.last_accepted_pre_prepare_time = None

        # Keeps a map of PRE-PREPAREs which did not satisfy timestamp
        # criteria, they can be accepted if >f PREPAREs are encountered.
        # This is emptied on view change. With each PRE-PREPARE, a flag is
        # stored which indicates whether there are sufficient acceptable
        # PREPAREs or not
        self.pre_prepares_stashed_for_incorrect_time = OrderedDict()

        self._bls_bft_replica = bls_bft_replica
        self._state_root_serializer = state_roots_serializer

        HookManager.__init__(self, ReplicaHooks.get_all_vals())

    def register_ledger(self, ledger_id):
        # Using ordered set since after ordering each PRE-PREPARE,
        # the request key is removed, so fast lookup and removal of
        # request key is needed. Need the collection to be ordered since
        # the request key needs to be removed once its ordered
        if ledger_id not in self.requestQueues:
            self.requestQueues[ledger_id] = OrderedSet()

    def ledger_uncommitted_size(self, ledgerId):
        if not self.isMaster:
            return None
        return self.node.getLedger(ledgerId).uncommitted_size

    def txnRootHash(self, ledger_str, to_str=True):
        if not self.isMaster:
            return None
        ledger = self.node.getLedger(ledger_str)
        h = ledger.uncommittedRootHash
        # If no uncommittedHash since this is the beginning of the tree
        # or no transactions affecting the ledger were made after the
        # last changes were committed
        root = h if h else ledger.tree.root_hash
        if to_str:
            root = ledger.hashToStr(root)
        return root

    def stateRootHash(self, ledger_id, to_str=True, committed=False):
        if not self.isMaster:
            return None
        state = self.node.getState(ledger_id)
        root = state.committedHeadHash if committed else state.headHash
        if to_str:
            root = self._state_root_serializer.serialize(bytes(root))
        return root

    @property
    def h(self) -> int:
        return self._h

    @h.setter
    def h(self, n):
        self._h = n
        self.H = self._h + self.config.LOG_SIZE
        self.logger.info('{} set watermarks as {} {}'.format(self, self.h, self.H))

    @property
    def last_ordered_3pc(self) -> tuple:
        return self._last_ordered_3pc

    @last_ordered_3pc.setter
    def last_ordered_3pc(self, key3PC):
        self._last_ordered_3pc = key3PC
        self.logger.info('{} set last ordered as {}'.format(
            self, self._last_ordered_3pc))

    @property
    def lastPrePrepareSeqNo(self):
        return self._lastPrePrepareSeqNo

    @lastPrePrepareSeqNo.setter
    def lastPrePrepareSeqNo(self, n):
        """
        This will _lastPrePrepareSeqNo to values greater than its previous
        values else it will not. To forcefully override as in case of `revert`,
        directly set `self._lastPrePrepareSeqNo`
        """
        if n > self._lastPrePrepareSeqNo:
            self._lastPrePrepareSeqNo = n
        else:
            self.logger.debug(
                '{} cannot set lastPrePrepareSeqNo to {} as its '
                'already {}'.format(
                    self, n, self._lastPrePrepareSeqNo))

    @property
    def requests(self):
        return self.node.requests

    @property
    def ledger_ids(self):
        return self.node.ledger_ids

    @property
    def quorums(self):
        return self.node.quorums

    @property
    def utc_epoch(self):
        return self.node.utc_epoch()

    # This is to enable replaying, inst_id, view_no and pp_seq_no are used
    # while replaying
    def get_utc_epoch_for_preprepare(self, inst_id, view_no, pp_seq_no):
        tm = self.utc_epoch
        if self.last_accepted_pre_prepare_time and \
                tm < self.last_accepted_pre_prepare_time:
            tm = self.last_accepted_pre_prepare_time
        return tm

    @staticmethod
    def generateName(nodeName: str, instId: int):
        """
        Create and return the name for a replica using its nodeName and
        instanceId.
         Ex: Alpha:1
        """

        if isinstance(nodeName, str):
            # Because sometimes it is bytes (why?)
            if ":" in nodeName:
                # Because in some cases (for requested messages) it
                # already has ':'. This should be fixed.
                return nodeName
        return "{}:{}".format(nodeName, instId)

    @staticmethod
    def getNodeName(replicaName: str):
        return replicaName.split(":")[0]

    @property
    def isPrimary(self):
        """
        Is this node primary?

        :return: True if this node is primary, False if not, None if primary status not known
        """
        return self._primaryName == self.name if self._primaryName is not None \
            else None

    @property
    def hasPrimary(self):
        return self.primaryName is not None

    @property
    def primaryName(self):
        """
        Name of the primary replica of this replica's instance

        :return: Returns name if primary is known, None otherwise
        """
        return self._primaryName

    @primaryName.setter
    def primaryName(self, value: Optional[str]) -> None:
        """
        Set the value of isPrimary.

        :param value: the value to set isPrimary to
        """
        self.primaryNames[self.viewNo] = value
        self.compact_primary_names()
        if value != self._primaryName:
            self._primaryName = value
            self.logger.info("{} setting primaryName for view no {} to: {}".
                             format(self, self.viewNo, value))
            if value is None:
                # Since the GC needs to happen after a primary has been
                # decided.
                return
            self._gc_before_new_view()
            if self.viewNo > 0:
                self._reset_watermarks_before_new_view()
            self._stateChanged()

    def compact_primary_names(self):
        min_allowed_view_no = self.viewNo - 1
        views_to_remove = []
        for view_no in self.primaryNames:
            if view_no >= min_allowed_view_no:
                break
            views_to_remove.append(view_no)
        for view_no in views_to_remove:
            self.primaryNames.pop(view_no)

    def primaryChanged(self, primaryName):
        self.batches.clear()
        if self.isMaster:
            # Since there is no temporary state data structure and state root
            # is explicitly set to correct value
            for lid in self.ledger_ids:
                try:
                    ledger = self.node.getLedger(lid)
                except KeyError:
                    continue
                ledger.reset_uncommitted()

        self.primaryName = primaryName
        self._setup_for_non_master_after_view_change(self.viewNo)

    def on_view_change_start(self):
        if self.isMaster:
            lst = self.last_prepared_certificate_in_view()
            self.last_prepared_before_view_change = lst
            self.logger.info('{} setting last prepared for master to {}'.format(self, lst))
        # It can be that last_ordered_3pc was set for the previous view, since it's set during catch-up
        # Example: a Node has last_ordered = (1, 300), and then the whole pool except this node restarted
        # The new viewNo is 0, but last_ordered is (1, 300), so all new requests will be discarded by this Node
        # if we don't reset last_ordered_3pc
        if self.viewNo <= self.last_ordered_3pc[0]:
            self.last_ordered_3pc = (self.viewNo, 0)

    def on_view_change_done(self):
        if not self.isMaster:
            raise LogicError("{} is not a master".format(self))
        self.last_prepared_before_view_change = None

    def clear_requests_and_fix_last_ordered(self):
        reqs_for_remove = []
        for key in self.requests:
            ledger_id, seq_no = self.node.seqNoDB.get(key)
            if seq_no is not None:
                reqs_for_remove.append((key, ledger_id, seq_no))
        for key, ledger_id, seq_no in reqs_for_remove:
            self.requests.free(key)
            self.requestQueues[int(ledger_id)].discard(key)
        master_last_ordered_3pc = self.node.master_replica.last_ordered_3pc
        if compare_3PC_keys(master_last_ordered_3pc, self.last_ordered_3pc) < 0:
            self.last_ordered_3pc = master_last_ordered_3pc

    def on_propagate_primary_done(self):
        if self.isMaster:
            # if this is a Primary that is re-connected (that is view change is not actually changed,
            # we just propagate it, then make sure that we don;t break the sequence
            # of ppSeqNo
            self.update_watermark_from_3pc()
            if self.isPrimary and (self.last_ordered_3pc[0] == self.viewNo):
                self.lastPrePrepareSeqNo = self.last_ordered_3pc[1]
        elif not self.isPrimary:
            self.h = 0
            self.H = sys.maxsize

    def get_lowest_probable_prepared_certificate_in_view(
            self, view_no) -> Optional[int]:
        """
        Return lowest pp_seq_no of the view for which can be prepared but
        choose from unprocessed PRE-PREPAREs and PREPAREs.
        """
        # TODO: Naive implementation, dont need to iterate over the complete
        # data structures, fix this later
        seq_no_pp = SortedList()  # pp_seq_no of PRE-PREPAREs
        # pp_seq_no of PREPAREs with count of PREPAREs for each
        seq_no_p = set()

        for (v, p) in self.prePreparesPendingPrevPP:
            if v == view_no:
                seq_no_pp.add(p)
            if v > view_no:
                break

        for (v, p), pr in self.preparesWaitingForPrePrepare.items():
            if v == view_no and len(pr) >= self.quorums.prepare.value:
                seq_no_p.add(p)

        for n in seq_no_pp:
            if n in seq_no_p:
                return n
        return None

    def _setup_last_ordered_for_non_master(self):
        """
        Since last ordered view_no and pp_seq_no are only communicated for
        master instance, backup instances use this method for restoring
        `last_ordered_3pc`
        :return:
        """
        if not self.isMaster and self.last_ordered_3pc[1] == 0 and\
                not self.isPrimary:
            # If not master instance choose last ordered seq no to be 1 less
            # the lowest prepared certificate in this view
            lowest_prepared = self.get_lowest_probable_prepared_certificate_in_view(
                self.viewNo)
            if lowest_prepared is not None:
                # now after catch up we have in last_ordered_3pc[1] value 0
                # it value should change last_ordered_3pc to lowest_prepared - 1
                self.logger.info('{} Setting last ordered for non-master as {}'.
                                 format(self, self.last_ordered_3pc))
                self.last_ordered_3pc = (self.viewNo, lowest_prepared - 1)
                self.update_watermark_from_3pc()

    def _setup_for_non_master_after_view_change(self, current_view):
        if not self.isMaster:
            for v in list(self.stashed_out_of_order_commits.keys()):
                if v < current_view:
                    self.stashed_out_of_order_commits.pop(v)

    def is_primary_in_view(self, viewNo: int) -> Optional[bool]:
        """
        Return whether this replica was primary in the given view
        """
        if viewNo not in self.primaryNames:
            return False
        return self.primaryNames[viewNo] == self.name

    def isMsgForCurrentView(self, msg):
        """
        Return whether this request's view number is equal to the current view
        number of this replica.
        """
        viewNo = getattr(msg, "viewNo", None)
        return viewNo == self.viewNo

    def isPrimaryForMsg(self, msg) -> Optional[bool]:
        """
        Return whether this replica is primary if the request's view number is
        equal this replica's view number and primary has been selected for
        the current view.
        Return None otherwise.
        :param msg: message
        """
        return self.isPrimary if self.isMsgForCurrentView(msg) \
            else self.is_primary_in_view(msg.viewNo)

    def isMsgFromPrimary(self, msg, sender: str) -> bool:
        """
        Return whether this message was from primary replica
        :param msg:
        :param sender:
        :return:
        """
        if self.isMsgForCurrentView(msg):
            return self.primaryName == sender
        try:
            return self.primaryNames[msg.viewNo] == sender
        except KeyError:
            return False

    def _stateChanged(self):
        """
        A series of actions to be performed when the state of this replica
        changes.

        - UnstashInBox (see _unstashInBox)
        """
        self._unstashInBox()
        if self.isPrimary is not None:
            try:
                self.processPostElectionMsgs()
            except SuspiciousNode as ex:
                self.outBox.append(ex)
                self.discard(ex.msg, ex.reason, self.logger.warning)

    def _stashInBox(self, msg):
        """
        Stash the specified message into the inBoxStash of this replica.

        :param msg: the message to stash
        """
        self.inBoxStash.append(msg)

    def _unstashInBox(self):
        """
        Append the inBoxStash to the right of the inBox.
        """
        # The stashed values need to go in "front" of the inBox.
        self.inBox.extendleft(self.inBoxStash)
        self.inBoxStash.clear()

    def __repr__(self):
        return self.name

    @property
    def f(self) -> int:
        """
        Return the number of Byzantine Failures that can be tolerated by this
        system. Equal to (N - 1)/3, where N is the number of nodes in the
        system.
        """
        return self.node.f

    @property
    def viewNo(self):
        """
        Return the current view number of this replica.
        """
        return self.node.viewNo

    def trackBatches(self, pp: PrePrepare, prevStateRootHash):
        # pp.discarded indicates the index from where the discarded requests
        #  starts hence the count of accepted requests, prevStateRoot is
        # tracked to revert this PRE-PREPARE
        self.logger.trace('{} tracking batch for {} with state root {}'.format(
            self, pp, prevStateRootHash))
        self.batches[(pp.viewNo, pp.ppSeqNo)] = [pp.ledgerId, pp.discarded,
                                                 pp.ppTime, prevStateRootHash]

    def send3PCBatch(self):
        r = 0
        for lid, q in self.requestQueues.items():
            # TODO: make the condition more apparent
            if len(q) >= self.config.Max3PCBatchSize or \
                    (self.lastBatchCreated + self.config.Max3PCBatchWait < time.perf_counter() and
                     len(q) > 0):
                oldStateRootHash = self.stateRootHash(lid, to_str=False)
                ppReq = self.create3PCBatch(lid)
                if ppReq is None:
                    continue
                self.sendPrePrepare(ppReq)
                self.trackBatches(ppReq, oldStateRootHash)
                r += 1

        if r > 0:
            self.lastBatchCreated = time.perf_counter()
        return r

    @staticmethod
    def batchDigest(reqs):
        return sha256(b''.join([r.digest.encode() for r in reqs])).hexdigest()

    def processReqDuringBatch(
            self,
            req: Request,
            cons_time: int,
            validReqs: List,
            inValidReqs: List,
            rejects: List):
        """
        This method will do dynamic validation and apply requests, also it
        will modify `validReqs`, `inValidReqs` and `rejects`
        """
        try:
            if self.isMaster:
                self.node.doDynamicValidation(req)
                self.node.applyReq(req, cons_time)
        except (InvalidClientMessageException, UnknownIdentifier) as ex:
            self.logger.warning('{} encountered exception {} while processing {}, '
                                'will reject'.format(self, ex, req))
            rejects.append((req.key, Reject(req.identifier, req.reqId, ex)))
            inValidReqs.append(req)
        else:
            validReqs.append(req)

    def create3PCBatch(self, ledger_id):
        pp_seq_no = self.lastPrePrepareSeqNo + 1
        self.logger.debug("{} creating batch {} for ledger {} with state root {}".format(
            self, pp_seq_no, ledger_id,
            self.stateRootHash(ledger_id, to_str=False)))

        if self.last_accepted_pre_prepare_time is None:
            last_ordered_ts = self._get_last_timestamp_from_state(ledger_id)
            if last_ordered_ts:
                self.last_accepted_pre_prepare_time = last_ordered_ts

        validReqs, inValidReqs, rejects, tm = self.consume_req_queue_for_pre_prepare(
            ledger_id, self.viewNo,
            pp_seq_no)
        if not (validReqs or inValidReqs):
            self.logger.trace('{} not creating a Pre-Prepare for view no {} '
                              'seq no {}'.format(self, self.viewNo, pp_seq_no))
            return

        reqs = validReqs + inValidReqs
        digest = self.batchDigest(reqs)

        state_root_hash = self.stateRootHash(ledger_id)
        params = [
            self.instId,
            self.viewNo,
            pp_seq_no,
            tm,
            [req.digest for req in reqs],
            len(validReqs),
            digest,
            ledger_id,
            state_root_hash,
            self.txnRootHash(ledger_id)
        ]

        # BLS multi-sig:
        params = self._bls_bft_replica.update_pre_prepare(params, ledger_id)

        pre_prepare = PrePrepare(*params)
        if self.isMaster:
            rv = self.execute_hook(ReplicaHooks.CREATE_PPR, pre_prepare)
            pre_prepare = rv if rv is not None else pre_prepare

        self.logger.trace('{} created a PRE-PREPARE with {} requests for ledger {}'.format(
            self, len(validReqs), ledger_id))
        self.lastPrePrepareSeqNo = pp_seq_no
        self.last_accepted_pre_prepare_time = tm
        if self.isMaster:
            self.outBox.extend(rejects)
            self.node.onBatchCreated(
                ledger_id, self.stateRootHash(
                    ledger_id, to_str=False))
        return pre_prepare

    def consume_req_queue_for_pre_prepare(self, ledger_id, view_no, pp_seq_no):
        # DO NOT REMOVE `view_no` argument, used while replay
        # tm = self.utc_epoch
        tm = self.get_utc_epoch_for_preprepare(self.instId, view_no,
                                               pp_seq_no)
        validReqs = []
        inValidReqs = []
        rejects = []
        while len(validReqs) + len(inValidReqs) < self.config.Max3PCBatchSize \
                and self.requestQueues[ledger_id]:
            key = self.requestQueues[ledger_id].pop(0)
            if key in self.requests:
                fin_req = self.requests[key].finalised
                self.processReqDuringBatch(
                    fin_req, tm, validReqs, inValidReqs, rejects)
            else:
                self.logger.debug('{} found {} in its request queue but the '
                                  'corresponding request was removed'.format(self, key))

        return validReqs, inValidReqs, rejects, tm

    def sendPrePrepare(self, ppReq: PrePrepare):
        self.sentPrePrepares[ppReq.viewNo, ppReq.ppSeqNo] = ppReq
        self.send(ppReq, TPCStat.PrePrepareSent)

    def readyFor3PC(self, key: ReqKey):
        fin_req = self.requests[key.digest].finalised
        queue = self.requestQueues[self.node.ledger_id_for_request(fin_req)]
        queue.add(key.digest)
        if not self.hasPrimary and len(queue) >= self.HAS_NO_PRIMARY_WARN_THRESCHOLD:
            self.logger.warning('{} is getting requests but still does not have '
                                'a primary so the replica will not process the request '
                                'until a primary is chosen'.format(self))

    def serviceQueues(self, limit=None):
        """
        Process `limit` number of messages in the inBox.

        :param limit: the maximum number of messages to process
        :return: the number of messages successfully processed
        """
        # TODO should handle SuspiciousNode here
        r = self.dequeue_pre_prepares() if self.node.isParticipating else 0
        r += self.inBoxRouter.handleAllSync(self.inBox, limit)
        r += self.send3PCBatch() if (self.isPrimary and
                                     self.node.isParticipating) else 0
        r += self._serviceActions()
        return r
        # Messages that can be processed right now needs to be added back to the
        # queue. They might be able to be processed later

    def processPostElectionMsgs(self):
        """
        Process messages waiting for the election of a primary replica to
        complete.
        """
        while self.postElectionMsgs:
            msg = self.postElectionMsgs.popleft()
            self.logger.debug("{} processing pended msg {}".format(self, msg))
            self.dispatchThreePhaseMsg(*msg)

    def dispatchThreePhaseMsg(self, msg: ThreePhaseMsg, sender: str) -> Any:
        """
        Create a three phase request to be handled by the threePhaseRouter.

        :param msg: the ThreePhaseMsg to dispatch
        :param sender: the name of the node that sent this request
        """
        senderRep = self.generateName(sender, self.instId)
        if self.isPpSeqNoStable(msg.ppSeqNo):
            self.discard(msg, "achieved stable checkpoint for 3 phase message", self.logger.info)
            return

        if self.has_already_ordered(msg.viewNo, msg.ppSeqNo):
            self.discard(msg, 'already ordered 3 phase message', self.logger.trace)
            return

        if self.isPpSeqNoBetweenWaterMarks(msg.ppSeqNo):
            if self.can_pp_seq_no_be_in_view(msg.viewNo, msg.ppSeqNo):
                self.threePhaseRouter.handleSync((msg, senderRep))
            else:
                self.discard(msg, 'un-acceptable pp seq no from previous view', self.logger.warning)
        else:
            self.logger.debug("{} stashing 3 phase message {} since ppSeqNo {} is not between {} and {}".
                              format(self, msg, msg.ppSeqNo, self.h, self.H))
            self.stashOutsideWatermarks((msg, sender))

    def processThreePhaseMsg(self, msg: ThreePhaseMsg, sender: str):
        """
        Process a 3-phase (pre-prepare, prepare and commit) request.
        Dispatch the request only if primary has already been decided, otherwise
        stash it.

        :param msg: the Three Phase message, one of PRE-PREPARE, PREPARE,
            COMMIT
        :param sender: name of the node that sent this message
        """
        if self.isPrimary is None:
            if not self.can_process_since_view_change_in_progress(msg):
                self.postElectionMsgs.append((msg, sender))
                self.logger.debug("Replica {} pended request {} from {}".format(self, msg, sender))
                return
        self.dispatchThreePhaseMsg(msg, sender)

    def can_process_since_view_change_in_progress(self, msg):
        # Commit msg with 3PC key not greater than last prepared one's
        r = isinstance(msg, Commit) and \
            self.last_prepared_before_view_change and \
            compare_3PC_keys((msg.viewNo, msg.ppSeqNo),
                             self.last_prepared_before_view_change) >= 0
        if r:
            self.logger.debug('{} can process {} since view change is in progress'.format(self, msg))
        return r

    def _process_valid_preprepare(self, pre_prepare, sender):
        # TODO: rename to apply_pre_prepare
        if not self.node.isParticipating:
            self.discard(pre_prepare, 'node is not participating', self.logger.debug)
            return None
        pre_state_root = self.stateRootHash(pre_prepare.ledgerId, to_str=False)
        why_not_applied = self._apply_pre_prepare(pre_prepare, sender)
        if why_not_applied is not None:
            return why_not_applied
        self.addToPrePrepares(pre_prepare)

        if self.isMaster:
            # TODO: can pre_state_root be used here instead?
            state_root = self.stateRootHash(pre_prepare.ledgerId, to_str=False)
            self.node.onBatchCreated(pre_prepare.ledgerId, state_root)
            # BLS multi-sig:
            self._bls_bft_replica.process_pre_prepare(pre_prepare, sender)
            self.logger.trace("{} saved shared multi signature for "
                              "root".format(self, pre_state_root))

        self.trackBatches(pre_prepare, pre_state_root)
        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        self.logger.debug("{} processed incoming PRE-PREPARE{}".format(self, key),
                          extra={"tags": ["processing"]})
        return None

    def processPrePrepare(self, pre_prepare: PrePrepare, sender: str):
        """
        Validate and process provided PRE-PREPARE, create and
        broadcast PREPARE for it.

        :param pre_prepare: message
        :param sender: name of the node that sent this message
        """
        key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
        self.logger.debug("{} received PRE-PREPARE{} from {}".format(self, key, sender))

        # TODO: should we still do it?
        # Converting each req_idrs from list to tuple
        req_idrs = {f.REQ_IDR.nm: [key for key in pre_prepare.reqIdr]}
        pre_prepare = updateNamedTuple(pre_prepare, **req_idrs)

        def report_suspicious(reason):
            ex = SuspiciousNode(sender, reason, pre_prepare)
            self.node.reportSuspiciousNodeEx(ex)

        why_not = self._can_process_pre_prepare(pre_prepare, sender)
        if why_not is None:
            why_not_applied = \
                self._process_valid_preprepare(pre_prepare, sender)
            if why_not_applied is not None:
                if why_not_applied == PP_APPLY_REJECT_WRONG:
                    report_suspicious(Suspicions.PPR_REJECT_WRONG)
                elif why_not_applied == PP_APPLY_WRONG_DIGEST:
                    report_suspicious(Suspicions.PPR_DIGEST_WRONG)
                elif why_not_applied == PP_APPLY_WRONG_STATE:
                    report_suspicious(Suspicions.PPR_STATE_WRONG)
                elif why_not_applied == PP_APPLY_ROOT_HASH_MISMATCH:
                    report_suspicious(Suspicions.PPR_TXN_WRONG)
                elif why_not_applied == PP_APPLY_HOOK_ERROR:
                    report_suspicious(Suspicions.PPR_PLUGIN_EXCEPTION)
        elif why_not == PP_CHECK_NOT_FROM_PRIMARY:
            report_suspicious(Suspicions.PPR_FRM_NON_PRIMARY)
        elif why_not == PP_CHECK_TO_PRIMARY:
            report_suspicious(Suspicions.PPR_TO_PRIMARY)
        elif why_not == PP_CHECK_DUPLICATE:
            report_suspicious(Suspicions.DUPLICATE_PPR_SENT)
        elif why_not == PP_CHECK_OLD:
            self.logger.debug("PRE-PREPARE {} has ppSeqNo lower "
                              "then the latest one - ignoring it".format(key))
        elif why_not == PP_CHECK_REQUEST_NOT_FINALIZED:
            non_fin_reqs = self.nonFinalisedReqs(pre_prepare.reqIdr)
            self.enqueue_pre_prepare(pre_prepare, sender, non_fin_reqs)
            # TODO: An optimisation might be to not request PROPAGATEs
            # if some PROPAGATEs are present or a client request is
            # present and sufficient PREPAREs and PRE-PREPARE are present,
            # then the digest can be compared but this is expensive as the
            # PREPARE and PRE-PREPARE contain a combined digest
            self.node.request_propagates(non_fin_reqs)
        elif why_not == PP_CHECK_NOT_NEXT:
            pp_view_no = pre_prepare.viewNo
            pp_seq_no = pre_prepare.ppSeqNo
            last_pp_view_no, last_pp_seq_no = self.__last_pp_3pc
            if pp_view_no >= last_pp_view_no and (
                    self.isMaster or self.last_ordered_3pc[1] != 0):
                seq_frm = last_pp_seq_no + 1 if pp_view_no == last_pp_view_no else 1
                seq_to = pp_seq_no - 1
                if seq_to >= seq_frm >= pp_seq_no - CHK_FREQ + 1:
                    self.logger.warning(
                        "{} missing PRE-PREPAREs from {} to {}, "
                        "going to request".format(self, seq_frm, seq_to))
                    self._request_missing_three_phase_messages(
                        pp_view_no, seq_frm, seq_to)
            self.enqueue_pre_prepare(pre_prepare, sender)
            self._setup_last_ordered_for_non_master()
        elif why_not == PP_CHECK_WRONG_TIME:
            key = (pre_prepare.viewNo, pre_prepare.ppSeqNo)
            item = (pre_prepare, sender, False)
            self.pre_prepares_stashed_for_incorrect_time[key] = item
            report_suspicious(Suspicions.PPR_TIME_WRONG)
        elif why_not == BlsBftReplica.PPR_BLS_MULTISIG_WRONG:
            report_suspicious(Suspicions.PPR_BLS_MULTISIG_WRONG)
        else:
            self.logger.warning("Unknown PRE-PREPARE check status: {}".format(why_not))

    def tryPrepare(self, pp: PrePrepare):
        """
        Try to send the Prepare message if the PrePrepare message is ready to
        be passed into the Prepare phase.
        """
        rv, msg = self.canPrepare(pp)
        if rv:
            self.doPrepare(pp)
        else:
            self.logger.debug("{} cannot send PREPARE since {}".format(self, msg))

    def processPrepare(self, prepare: Prepare, sender: str) -> None:
        """
        Validate and process the PREPARE specified.
        If validation is successful, create a COMMIT and broadcast it.

        :param prepare: a PREPARE msg
        :param sender: name of the node that sent the PREPARE
        """
        key = (prepare.viewNo, prepare.ppSeqNo)
        self.logger.debug("{} received PREPARE{} from {}".format(self, key, sender))

        # TODO move this try/except up higher
        if self.isPpSeqNoStable(prepare.ppSeqNo):
            self.discard(prepare,
                         "achieved stable checkpoint for Prepare",
                         self.logger.debug)
            return
        try:
            if self.validatePrepare(prepare, sender):
                self.addToPrepares(prepare, sender)
                self.stats.inc(TPCStat.PrepareRcvd)
                self.logger.debug("{} processed incoming PREPARE {}".format(
                    self, (prepare.viewNo, prepare.ppSeqNo)))
            else:
                # TODO let's have isValidPrepare throw an exception that gets
                # handled and possibly logged higher
                self.logger.trace("{} cannot process incoming PREPARE".format(self))
        except SuspiciousNode as ex:
            self.node.reportSuspiciousNodeEx(ex)

    def processCommit(self, commit: Commit, sender: str) -> None:
        """
        Validate and process the COMMIT specified.
        If validation is successful, return the message to the node.

        :param commit: an incoming COMMIT message
        :param sender: name of the node that sent the COMMIT
        """
        self.logger.debug("{} received COMMIT{} from {}".format(
            self, (commit.viewNo, commit.ppSeqNo), sender))

        if self.isPpSeqNoStable(commit.ppSeqNo):
            self.discard(commit,
                         "achieved stable checkpoint for Commit",
                         self.logger.debug)
            return

        if self.validateCommit(commit, sender):
            self.stats.inc(TPCStat.CommitRcvd)
            self.addToCommits(commit, sender)
            self.logger.debug("{} processed incoming COMMIT{}".format(
                self, (commit.viewNo, commit.ppSeqNo)))

    def tryCommit(self, prepare: Prepare):
        """
        Try to commit if the Prepare message is ready to be passed into the
        commit phase.
        """
        rv, reason = self.canCommit(prepare)
        if rv:
            self.doCommit(prepare)
        else:
            self.logger.debug("{} cannot send COMMIT since {}".format(self, reason))

    def tryOrder(self, commit: Commit):
        """
        Try to order if the Commit message is ready to be ordered.
        """
        canOrder, reason = self.canOrder(commit)
        if canOrder:
            self.logger.trace("{} returning request to node".format(self))
            self.doOrder(commit)
        else:
            self.logger.debug("{} cannot return request to node: {}".format(self, reason))
        return canOrder

    def doPrepare(self, pp: PrePrepare):
        self.logger.debug("{} Sending PREPARE{} at {}".format(
            self, (pp.viewNo, pp.ppSeqNo), time.perf_counter()))
        params = [self.instId,
                  pp.viewNo,
                  pp.ppSeqNo,
                  pp.ppTime,
                  pp.digest,
                  pp.stateRootHash,
                  pp.txnRootHash]

        # BLS multi-sig:
        params = self._bls_bft_replica.update_prepare(params, pp.ledgerId)

        prepare = Prepare(*params)
        if self.isMaster:
            rv = self.execute_hook(ReplicaHooks.CREATE_PR, prepare, pp)
            prepare = rv if rv is not None else prepare
        self.send(prepare, TPCStat.PrepareSent)
        self.addToPrepares(prepare, self.name)

    def doCommit(self, p: Prepare):
        """
        Create a commit message from the given Prepare message and trigger the
        commit phase
        :param p: the prepare message
        """
        key_3pc = (p.viewNo, p.ppSeqNo)
        self.logger.debug("{} Sending COMMIT{} at {}".format(self, key_3pc, time.perf_counter()))

        params = [
            self.instId, p.viewNo, p.ppSeqNo
        ]

        pre_prepare = self.getPrePrepare(*key_3pc)

        # BLS multi-sig:
        if p.stateRootHash is not None:
            pre_prepare = self.getPrePrepare(*key_3pc)
            params = self._bls_bft_replica.update_commit(params, pre_prepare)

        commit = Commit(*params)
        if self.isMaster:
            rv = self.execute_hook(ReplicaHooks.CREATE_CM, commit)
            commit = rv if rv is not None else commit

        self.send(commit, TPCStat.CommitSent)
        self.addToCommits(commit, self.name)

    def nonFinalisedReqs(self, reqKeys: List[Tuple[str, int]]):
        """
        Check if there are any requests which are not finalised, i.e for
        which there are not enough PROPAGATEs
        """
        return {key for key in reqKeys if not self.requests.is_finalised(key)}

    def __is_next_pre_prepare(self, view_no: int, pp_seq_no: int):
        if view_no == self.viewNo and pp_seq_no == 1:
            # First PRE-PREPARE in a new view
            return True
        (last_pp_view_no, last_pp_seq_no) = self.__last_pp_3pc
        if last_pp_view_no > view_no:
            return False
        if last_pp_view_no < view_no:
            # TODO: strange assumption here ???
            if view_no != self.viewNo:
                raise LogicError(
                    "{} 'view_no' {} is not equal to current view_no {}"
                    .format(self, view_no, self.viewNo)
                )
            last_pp_seq_no = 0
        if pp_seq_no - last_pp_seq_no > 1:
            return False
        return True

    @property
    def __last_pp_3pc(self):
        last_pp = self.lastPrePrepare
        if not last_pp:
            return self.last_ordered_3pc

        last_3pc = (last_pp.viewNo, last_pp.ppSeqNo)
        if compare_3PC_keys(self.last_ordered_3pc, last_3pc) > 0:
            return last_3pc

        return self.last_ordered_3pc

    def revert(self, ledgerId, stateRootHash, reqCount):
        # A batch should only be reverted if all batches that came after it
        # have been reverted
        ledger = self.node.getLedger(ledgerId)
        state = self.node.getState(ledgerId)
        self.logger.info('{} reverting {} txns and state root from {} to {} for'
                         ' ledger {}'.format(self, reqCount, state.headHash, stateRootHash, ledgerId))
        state.revertToHead(stateRootHash)
        ledger.discardTxns(reqCount)
        self.node.onBatchRejected(ledgerId)

    def _apply_pre_prepare(self, pre_prepare: PrePrepare, sender: str) -> Optional[int]:
        """
        Applies (but not commits) requests of the PrePrepare
        to the ledger and state
        """

        valid_reqs = []
        invalid_reqs = []
        rejects = []

        if self.isMaster:
            old_state_root = \
                self.stateRootHash(pre_prepare.ledgerId, to_str=False)
            old_txn_root = self.txnRootHash(pre_prepare.ledgerId)
            self.logger.debug('{} state root before processing {} is {}, {}'.format(
                self,
                pre_prepare,
                old_state_root,
                old_txn_root))

        for req_key in pre_prepare.reqIdr:
            req = self.requests[req_key].finalised

            self.processReqDuringBatch(req,
                                       pre_prepare.ppTime,
                                       valid_reqs,
                                       invalid_reqs,
                                       rejects)

        def revert():
            self.revert(pre_prepare.ledgerId,
                        old_state_root,
                        len(valid_reqs))

        if len(valid_reqs) != pre_prepare.discarded:
            if self.isMaster:
                revert()
            return PP_APPLY_REJECT_WRONG

        digest = self.batchDigest(valid_reqs + invalid_reqs)

        # A PRE-PREPARE is sent that does not match request digest
        if digest != pre_prepare.digest:
            if self.isMaster:
                revert()
            return PP_APPLY_WRONG_DIGEST

        if self.isMaster:
            if pre_prepare.stateRootHash != self.stateRootHash(pre_prepare.ledgerId):
                revert()
                return PP_APPLY_WRONG_STATE

            if pre_prepare.txnRootHash != self.txnRootHash(pre_prepare.ledgerId):
                revert()
                return PP_APPLY_ROOT_HASH_MISMATCH

            try:
                self.execute_hook(ReplicaHooks.APPLY_PPR, pre_prepare)
            except Exception as ex:
                self.logger.warning('{} encountered exception in replica '
                                    'hook {} : {}'.
                                    format(self, ReplicaHooks.APPLY_PPR, ex))
                revert()
                return PP_APPLY_HOOK_ERROR

            self.outBox.extend(rejects)
        return None

    def _can_process_pre_prepare(self, pre_prepare: PrePrepare, sender: str) -> Optional[int]:
        """
        Decide whether this replica is eligible to process a PRE-PREPARE.

        :param pre_prepare: a PRE-PREPARE msg to process
        :param sender: the name of the node that sent the PRE-PREPARE msg
        """
        # TODO: Check whether it is rejecting PRE-PREPARE from previous view

        # PRE-PREPARE should not be sent from non primary
        if not self.isMsgFromPrimary(pre_prepare, sender):
            return PP_CHECK_NOT_FROM_PRIMARY

        # A PRE-PREPARE is being sent to primary
        if self.isPrimaryForMsg(pre_prepare) is True:
            return PP_CHECK_TO_PRIMARY

        # Already has a PRE-PREPARE with same 3 phase key
        if (pre_prepare.viewNo, pre_prepare.ppSeqNo) in self.prePrepares:
            return PP_CHECK_DUPLICATE

        if not self.node.isParticipating:
            # Let the node stash the pre-prepare
            # TODO: The next processed pre-prepare needs to take consider if
            # the last pre-prepare was stashed or not since stashed requests
            # do not make change to state or ledger
            return None

        if compare_3PC_keys((pre_prepare.viewNo, pre_prepare.ppSeqNo),
                            self.__last_pp_3pc) > 0:
            return PP_CHECK_OLD  # ignore old pre-prepare

        if self.nonFinalisedReqs(pre_prepare.reqIdr):
            return PP_CHECK_REQUEST_NOT_FINALIZED

        if not self.is_pre_prepare_time_acceptable(pre_prepare):
            return PP_CHECK_WRONG_TIME

        if not self.__is_next_pre_prepare(pre_prepare.viewNo,
                                          pre_prepare.ppSeqNo):
            return PP_CHECK_NOT_NEXT

        # BLS multi-sig:
        status = self._bls_bft_replica.validate_pre_prepare(pre_prepare,
                                                            sender)
        if status is not None:
            return status
        return None

    def addToPrePrepares(self, pp: PrePrepare) -> None:
        """
        Add the specified PRE-PREPARE to this replica's list of received
        PRE-PREPAREs and try sending PREPARE

        :param pp: the PRE-PREPARE to add to the list
        """
        key = (pp.viewNo, pp.ppSeqNo)
        self.prePrepares[key] = pp
        self.lastPrePrepareSeqNo = pp.ppSeqNo
        self.last_accepted_pre_prepare_time = pp.ppTime
        self.dequeue_prepares(*key)
        self.dequeue_commits(*key)
        self.stats.inc(TPCStat.PrePrepareRcvd)
        self.tryPrepare(pp)

    def has_sent_prepare(self, request) -> bool:
        return self.prepares.hasPrepareFrom(request, self.name)

    def canPrepare(self, ppReq) -> (bool, str):
        """
        Return whether the batch of requests in the PRE-PREPARE can
        proceed to the PREPARE step.

        :param ppReq: any object with identifier and requestId attributes
        """
        if not self.node.isParticipating:
            return False, 'node is not participating'
        if self.has_sent_prepare(ppReq):
            return False, 'has already sent PREPARE for {}'.format(ppReq)
        return True, ''

    def validatePrepare(self, prepare: Prepare, sender: str) -> bool:
        """
        Return whether the PREPARE specified is valid.

        :param prepare: the PREPARE to validate
        :param sender: the name of the node that sent the PREPARE
        :return: True if PREPARE is valid, False otherwise
        """
        key = (prepare.viewNo, prepare.ppSeqNo)
        primaryStatus = self.isPrimaryForMsg(prepare)

        ppReq = self.getPrePrepare(*key)

        # If a non primary replica and receiving a PREPARE request before a
        # PRE-PREPARE request, then proceed

        # PREPARE should not be sent from primary
        if self.isMsgFromPrimary(prepare, sender):
            raise SuspiciousNode(sender, Suspicions.PR_FRM_PRIMARY, prepare)

        # If non primary replica
        if primaryStatus is False:
            if self.prepares.hasPrepareFrom(prepare, sender):
                raise SuspiciousNode(
                    sender, Suspicions.DUPLICATE_PR_SENT, prepare)
            # If PRE-PREPARE not received for the PREPARE, might be slow
            # network
            if not ppReq:
                self.enqueue_prepare(prepare, sender)
                self._setup_last_ordered_for_non_master()
                return False
        # If primary replica
        if primaryStatus is True:
            if self.prepares.hasPrepareFrom(prepare, sender):
                raise SuspiciousNode(
                    sender, Suspicions.DUPLICATE_PR_SENT, prepare)
            # If PRE-PREPARE was not sent for this PREPARE, certainly
            # malicious behavior
            elif not ppReq:
                raise SuspiciousNode(
                    sender, Suspicions.UNKNOWN_PR_SENT, prepare)

        if primaryStatus is None and not ppReq:
            self.enqueue_prepare(prepare, sender)
            self._setup_last_ordered_for_non_master()
            return False

        if prepare.digest != ppReq.digest:
            raise SuspiciousNode(sender, Suspicions.PR_DIGEST_WRONG, prepare)

        elif prepare.stateRootHash != ppReq.stateRootHash:
            raise SuspiciousNode(sender, Suspicions.PR_STATE_WRONG,
                                 prepare)
        elif prepare.txnRootHash != ppReq.txnRootHash:
            raise SuspiciousNode(sender, Suspicions.PR_TXN_WRONG,
                                 prepare)

        try:
            self.execute_hook(ReplicaHooks.VALIDATE_PR, prepare, ppReq)
        except Exception as ex:
            self.logger.warning('{} encountered exception in replica '
                                'hook {} : {}'.
                                format(self, ReplicaHooks.VALIDATE_PR, ex))
            raise SuspiciousNode(sender, Suspicions.PR_PLUGIN_EXCEPTION,
                                 prepare)

        # BLS multi-sig:
        self._bls_bft_replica.validate_prepare(prepare, sender)

        return True

    def addToPrepares(self, prepare: Prepare, sender: str):
        """
        Add the specified PREPARE to this replica's list of received
        PREPAREs and try sending COMMIT

        :param prepare: the PREPARE to add to the list
        """
        # BLS multi-sig:
        self._bls_bft_replica.process_prepare(prepare, sender)

        self.prepares.addVote(prepare, sender)
        self.dequeue_commits(prepare.viewNo, prepare.ppSeqNo)
        self.tryCommit(prepare)

    def getPrePrepare(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        if key in self.sentPrePrepares:
            return self.sentPrePrepares[key]
        if key in self.prePrepares:
            return self.prePrepares[key]
        return None

    def get_sent_prepare(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        if key in self.prepares:
            prepare = self.prepares[key].msg
            if self.prepares.hasPrepareFrom(prepare, self.name):
                return prepare
        return None

    def get_sent_commit(self, viewNo, ppSeqNo):
        key = (viewNo, ppSeqNo)
        if key in self.commits:
            commit = self.commits[key].msg
            if self.commits.hasCommitFrom(commit, self.name):
                return commit
        return None

    @property
    def lastPrePrepare(self):
        last_3pc = (0, 0)
        lastPp = None
        if self.sentPrePrepares:
            (v, s), pp = self.sentPrePrepares.peekitem(-1)
            last_3pc = (v, s)
            lastPp = pp
        if self.prePrepares:
            (v, s), pp = self.prePrepares.peekitem(-1)
            if compare_3PC_keys(last_3pc, (v, s)) > 0:
                lastPp = pp
        return lastPp

    def hasCommitted(self, request) -> bool:
        return self.commits.hasCommitFrom(ThreePhaseKey(
            request.viewNo, request.ppSeqNo), self.name)

    def canCommit(self, prepare: Prepare) -> (bool, str):
        """
        Return whether the specified PREPARE can proceed to the Commit
        step.

        Decision criteria:

        - If this replica has got just n-f-1 PREPARE requests then commit request.
        - If less than n-f-1 PREPARE requests then probably there's no consensus on
            the request; don't commit
        - If more than n-f-1 then already sent COMMIT; don't commit

        :param prepare: the PREPARE
        """
        if not self.node.isParticipating:
            return False, 'node is not participating'
        quorum = self.quorums.prepare.value
        if not self.prepares.hasQuorum(prepare, quorum):
            return False, 'does not have prepare quorum for {}'.format(prepare)
        if self.hasCommitted(prepare):
            return False, 'has already sent COMMIT for {}'.format(prepare)
        return True, ''

    def validateCommit(self, commit: Commit, sender: str) -> bool:
        """
        Return whether the COMMIT specified is valid.

        :param commit: the COMMIT to validate
        :return: True if `request` is valid, False otherwise
        """
        key = (commit.viewNo, commit.ppSeqNo)
        ppReq = self.getPrePrepare(*key)
        if not ppReq:
            self.enqueue_commit(commit, sender)
            return False

        # TODO: Fix problem that can occur with a primary and non-primary(s)
        # colluding and the honest nodes being slow
        if ((key not in self.prepares and key not in self.sentPrePrepares) and
                (key not in self.preparesWaitingForPrePrepare)):
            self.logger.error("{} rejecting COMMIT{} due to lack of prepares".format(self, key))
            return False

        if self.commits.hasCommitFrom(commit, sender):
            raise SuspiciousNode(sender, Suspicions.DUPLICATE_CM_SENT, commit)

        # BLS multi-sig:
        pre_prepare = self.getPrePrepare(commit.viewNo, commit.ppSeqNo)
        why_not = self._bls_bft_replica.validate_commit(commit, sender, pre_prepare)

        if why_not == BlsBftReplica.CM_BLS_SIG_WRONG:
            raise SuspiciousNode(sender,
                                 Suspicions.CM_BLS_SIG_WRONG,
                                 commit)
        elif why_not is not None:
            self.logger.warning("Unknown error code returned for bls commit "
                                "validation {}".format(why_not))

        return True

    def addToCommits(self, commit: Commit, sender: str):
        """
        Add the specified COMMIT to this replica's list of received
        commit requests.

        :param commit: the COMMIT to add to the list
        :param sender: the name of the node that sent the COMMIT
        """
        # BLS multi-sig:
        self._bls_bft_replica.process_commit(commit, sender)

        self.commits.addVote(commit, sender)
        self.tryOrder(commit)

    def canOrder(self, commit: Commit) -> Tuple[bool, Optional[str]]:
        """
        Return whether the specified commitRequest can be returned to the node.

        Decision criteria:

        - If have got just n-f Commit requests then return request to node
        - If less than n-f of commit requests then probably don't have
            consensus on the request; don't return request to node
        - If more than n-f then already returned to node; don't return request
            to node

        :param commit: the COMMIT
        """
        quorum = self.quorums.commit.value
        if not self.commits.hasQuorum(commit, quorum):
            return False, "no quorum ({}): {} commits where f is {}". \
                format(quorum, commit, self.f)

        key = (commit.viewNo, commit.ppSeqNo)
        if self.has_already_ordered(*key):
            return False, "already ordered"

        if commit.ppSeqNo > 1 and not self.all_prev_ordered(commit):
            viewNo, ppSeqNo = commit.viewNo, commit.ppSeqNo
            if viewNo not in self.stashed_out_of_order_commits:
                self.stashed_out_of_order_commits[viewNo] = {}
            self.stashed_out_of_order_commits[viewNo][ppSeqNo] = commit
            self.startRepeating(self.process_stashed_out_of_order_commits, 1)
            return False, "stashing {} since out of order". \
                format(commit)

        return True, None

    def all_prev_ordered(self, commit: Commit):
        """
        Return True if all previous COMMITs have been ordered
        """
        # TODO: This method does a lot of work, choose correct data
        # structures to make it efficient.

        viewNo, ppSeqNo = commit.viewNo, commit.ppSeqNo

        if self.last_ordered_3pc == (viewNo, ppSeqNo - 1):
            # Last ordered was in same view as this COMMIT
            return True

        # if some PREPAREs/COMMITs were completely missed in the same view
        toCheck = set()
        toCheck.update(set(self.sentPrePrepares.keys()))
        toCheck.update(set(self.prePrepares.keys()))
        toCheck.update(set(self.prepares.keys()))
        toCheck.update(set(self.commits.keys()))
        for (v, p) in toCheck:
            if v < viewNo and (v, p) not in self.ordered:
                # Have commits from previous view that are unordered.
                return False
            if v == viewNo and p < ppSeqNo and (v, p) not in self.ordered:
                # If unordered commits are found with lower ppSeqNo then this
                # cannot be ordered.
                return False

        return True

    def process_stashed_out_of_order_commits(self):
        # This method is called periodically to check for any commits that
        # were stashed due to lack of commits before them and orders them if it
        # can
        self.logger.debug('{} trying to order from out of order commits. '
                          'Len(stashed_out_of_order_commits) == {}'
                          .format(self, len(self.stashed_out_of_order_commits)))
        if self.last_ordered_3pc:
            lastOrdered = self.last_ordered_3pc
            vToRemove = set()
            for v in self.stashed_out_of_order_commits:
                if v < lastOrdered[0]:
                    self.logger.debug(
                        "{} found commits {} from previous view {}"
                        " that were not ordered but last ordered"
                        " is {}".format(
                            self, self.stashed_out_of_order_commits[v], v, lastOrdered))
                    vToRemove.add(v)
                    continue
                pToRemove = set()
                for p, commit in self.stashed_out_of_order_commits[v].items():
                    if (v, p) in self.ordered or\
                            self.has_already_ordered(*(commit.viewNo, commit.ppSeqNo)):
                        pToRemove.add(p)
                        continue
                    if (v == lastOrdered[0] and lastOrdered == (v, p - 1)) or \
                            (v > lastOrdered[0] and self.isLowestCommitInView(commit)):
                        self.logger.debug("{} ordering stashed commit {}".format(self, commit))
                        if self.tryOrder(commit):
                            lastOrdered = (v, p)
                            pToRemove.add(p)

                for p in pToRemove:
                    del self.stashed_out_of_order_commits[v][p]
                if not self.stashed_out_of_order_commits[v]:
                    vToRemove.add(v)

            for v in vToRemove:
                del self.stashed_out_of_order_commits[v]

            if not self.stashed_out_of_order_commits:
                self.stopRepeating(self.process_stashed_out_of_order_commits)
        else:
            self.logger.debug('{} last_ordered_3pc if False. '
                              'Len(stashed_out_of_order_commits) == {}'
                              .format(self, len(self.stashed_out_of_order_commits)))

    def isLowestCommitInView(self, commit):
        view_no = commit.viewNo
        if view_no > self.viewNo:
            self.logger.debug('{} encountered {} which belongs to a later view'.format(self, commit))
            return False
        return commit.ppSeqNo == 1

    def last_prepared_certificate_in_view(self) -> Optional[Tuple[int, int]]:
        # Pick the latest sent COMMIT in the view.
        # TODO: Consider stashed messages too?
        if not self.isMaster:
            raise LogicError("{} is not a master".format(self))
        keys = []
        quorum = self.quorums.prepare.value
        for key in self.prepares.keys():
            if self.prepares.hasQuorum(ThreePhaseKey(*key), quorum):
                keys.append(key)
        return max_3PC_key(keys) if keys else None

    def has_prepared(self, key):
        return self.getPrePrepare(*key) and self.prepares.hasQuorum(
            ThreePhaseKey(*key), self.quorums.prepare.value)

    def doOrder(self, commit: Commit):
        key = (commit.viewNo, commit.ppSeqNo)
        self.logger.debug("{} ordering COMMIT {}".format(self, key))
        return self.order_3pc_key(key)

    def order_3pc_key(self, key):
        pp = self.getPrePrepare(*key)
        if pp is None:
            raise ValueError(
                "{} no PrePrepare with a 'key' {} found"
                .format(self, key)
            )

        self.addToOrdered(*key)
        ordered = Ordered(self.instId,
                          pp.viewNo,
                          pp.reqIdr[:pp.discarded],
                          pp.ppSeqNo,
                          pp.ppTime,
                          pp.ledgerId,
                          pp.stateRootHash,
                          pp.txnRootHash)
        if self.isMaster:
            rv = self.execute_hook(ReplicaHooks.CREATE_ORD, ordered, pp)
            ordered = rv if rv is not None else ordered

        self._discard_ordered_req_keys(pp)

        self.send(ordered, TPCStat.OrderSent)
        self.logger.debug("{} ordered batch request, view no {}, ppSeqNo {}, "
                          "ledger {}, state root {}, txn root {}, requests ordered {}, discarded {}".
                          format(self, pp.viewNo, pp.ppSeqNo, pp.ledgerId,
                                 pp.stateRootHash, pp.txnRootHash, pp.reqIdr[:pp.discarded],
                                 pp.reqIdr[pp.discarded:]))
        self.logger.info("{} ordered batch request, view no {}, ppSeqNo {}, "
                         "ledger {}, state root {}, txn root {}, requests ordered {}, discarded {}".
                         format(self, pp.viewNo, pp.ppSeqNo, pp.ledgerId,
                                pp.stateRootHash, pp.txnRootHash, len(pp.reqIdr[:pp.discarded]),
                                len(pp.reqIdr[pp.discarded:])))

        self.addToCheckpoint(pp.ppSeqNo, pp.digest, pp.ledgerId)

        # BLS multi-sig:
        self._bls_bft_replica.process_order(key, self.quorums, pp)

        return True

    def _discard_ordered_req_keys(self, pp: PrePrepare):
        for k in pp.reqIdr:
            # Using discard since the key may not be present as in case of
            # primary, the key was popped out while creating PRE-PREPARE.
            # Or in case of node catching up, it will not validate
            # PRE-PREPAREs or PREPAREs but will only validate number of COMMITs
            #  and their consistency with PRE-PREPARE of PREPAREs
            self.discard_req_key(pp.ledgerId, k)

    def discard_req_key(self, ledger_id, req_key):
        self.requestQueues[ledger_id].discard(req_key)

    def processCheckpoint(self, msg: Checkpoint, sender: str) -> bool:
        """
        Process checkpoint messages

        :return: whether processed (True) or stashed (False)
        """
        self.logger.info('{} processing checkpoint {} from {}'.format(self, msg, sender))
        seqNoEnd = msg.seqNoEnd
        if self.isPpSeqNoStable(seqNoEnd):
            self.discard(msg, reason="Checkpoint already stable", logMethod=self.logger.debug)
            return True

        seqNoStart = msg.seqNoStart
        key = (seqNoStart, seqNoEnd)

        if key not in self.checkpoints or not self.checkpoints[key].digest:
            self.stashCheckpoint(msg, sender)
            self.__start_catchup_if_needed()
            return False

        checkpoint_state = self.checkpoints[key]
        # Raise the error only if master since only master's last
        # ordered 3PC is communicated during view change
        if self.isMaster and checkpoint_state.digest != msg.digest:
            self.logger.warning("{} received an incorrect digest {} for "
                                "checkpoint {} from {}".format(self, msg.digest, key, sender))
            return True

        checkpoint_state.receivedDigests[sender] = msg.digest
        self.checkIfCheckpointStable(key)
        return True

    def __start_catchup_if_needed(self):
        stashed_checkpoint_ends = self.stashed_checkpoints_with_quorum()
        lag_in_checkpoints = len(stashed_checkpoint_ends)
        if self.checkpoints:
            (s, e) = firstKey(self.checkpoints)
            # If the first stored own checkpoint has a not aligned lower bound
            # (this means that it was started after a catch-up), is complete
            # and there is a quorumed stashed checkpoint from other replicas
            # with the same end then don't include this stashed checkpoint
            # into the lag
            if s % self.config.CHK_FREQ != 0 \
                    and self.checkpoints[(s, e)].seqNo == e \
                    and e in stashed_checkpoint_ends:
                lag_in_checkpoints -= 1
        is_stashed_enough = \
            lag_in_checkpoints > self.STASHED_CHECKPOINTS_BEFORE_CATCHUP
        if not is_stashed_enough:
            return

        self.logger.display('{} has lagged for {} checkpoints so updating watermarks to {}'.
                            format(self, lag_in_checkpoints, stashed_checkpoint_ends[-1]))
        self.h = stashed_checkpoint_ends[-1]

        if self.isMaster and not self.isPrimary:
            self.logger.display('{} has lagged for {} checkpoints so the catchup procedure starts'.
                                format(self, lag_in_checkpoints))
            self.node.start_catchup()

    def addToCheckpoint(self, ppSeqNo, digest, ledger_id):
        for (s, e) in self.checkpoints.keys():
            if s <= ppSeqNo <= e:
                state = self.checkpoints[s, e]  # type: CheckpointState
                state.digests.append(digest)
                state = updateNamedTuple(state, seqNo=ppSeqNo)
                self.checkpoints[s, e] = state
                break
        else:
            s, e = ppSeqNo, math.ceil(ppSeqNo / self.config.CHK_FREQ) * self.config.CHK_FREQ
            self.logger.debug("{} adding new checkpoint state for {}".format(self, (s, e)))
            state = CheckpointState(ppSeqNo, [digest, ], None, {}, False)
            self.checkpoints[s, e] = state

        if state.seqNo == e:
            if len(state.digests) == self.config.CHK_FREQ:
                # TODO CheckpointState/Checkpoint is not a namedtuple anymore
                # 1. check if updateNamedTuple works for the new message type
                # 2. choose another name
                state = updateNamedTuple(state,
                                         digest=sha256(
                                             serialize_msg_for_signing(
                                                 state.digests)
                                         ).hexdigest(),
                                         digests=[])
                self.checkpoints[s, e] = state
                self.logger.info("{} sending Checkpoint {} view {} checkpointState digest {}. Ledger {} "
                                 "txn root hash {}. Committed state root hash {} Uncommitted state root hash {}".
                                 format(self, (s, e), self.viewNo, state.digest, ledger_id,
                                        self.txnRootHash(ledger_id), self.stateRootHash(ledger_id, committed=True),
                                        self.stateRootHash(ledger_id, committed=False)))
                self.send(Checkpoint(self.instId, self.viewNo, s, e, state.digest))
            self.processStashedCheckpoints((s, e))

    def markCheckPointStable(self, seqNo):
        previousCheckpoints = []
        for (s, e), state in self.checkpoints.items():
            if e == seqNo:
                # TODO CheckpointState/Checkpoint is not a namedtuple anymore
                # 1. check if updateNamedTuple works for the new message type
                # 2. choose another name
                state = updateNamedTuple(state, isStable=True)
                self.checkpoints[s, e] = state
                break
            else:
                previousCheckpoints.append((s, e))
        else:
            self.logger.debug("{} could not find {} in checkpoints".format(self, seqNo))
            return
        self.h = seqNo
        for k in previousCheckpoints:
            self.logger.trace("{} removing previous checkpoint {}".format(self, k))
            self.checkpoints.pop(k)
        self._remove_stashed_checkpoints(till_3pc_key=(self.viewNo, seqNo))
        self._gc((self.viewNo, seqNo))
        self.logger.info("{} marked stable checkpoint {}".format(self, (s, e)))
        self.processStashedMsgsForNewWaterMarks()

    def checkIfCheckpointStable(self, key: Tuple[int, int]):
        ckState = self.checkpoints[key]
        if self.quorums.checkpoint.is_reached(len(ckState.receivedDigests)):
            self.markCheckPointStable(ckState.seqNo)
            return True
        else:
            self.logger.debug('{} has state.receivedDigests as {}'.format(
                self, ckState.receivedDigests.keys()))
            return False

    def stashCheckpoint(self, ck: Checkpoint, sender: str):
        self.logger.debug('{} stashing {} from {}'.format(self, ck, sender))
        seqNoStart, seqNoEnd = ck.seqNoStart, ck.seqNoEnd
        if ck.viewNo not in self.stashedRecvdCheckpoints:
            self.stashedRecvdCheckpoints[ck.viewNo] = {}
        stashed_for_view = self.stashedRecvdCheckpoints[ck.viewNo]
        if (seqNoStart, seqNoEnd) not in stashed_for_view:
            stashed_for_view[seqNoStart, seqNoEnd] = {}
        stashed_for_view[seqNoStart, seqNoEnd][sender] = ck

    def _clear_prev_view_pre_prepares(self):
        to_remove = []
        for idx, (pp, _, _) in enumerate(self.prePreparesPendingFinReqs):
            if pp.viewNo < self.viewNo:
                to_remove.insert(0, idx)
        for idx in to_remove:
            self.prePreparesPendingFinReqs.pop(idx)

        for (v, p) in list(self.prePreparesPendingPrevPP.keys()):
            if v < self.viewNo:
                self.prePreparesPendingPrevPP.pop((v, p))

    def stashed_checkpoints_with_quorum(self):
        end_pp_seq_numbers = []
        quorum = self.quorums.checkpoint
        for (_, seq_no_end), senders in self.stashedRecvdCheckpoints.get(
                self.viewNo, {}).items():
            if quorum.is_reached(len(senders)):
                end_pp_seq_numbers.append(seq_no_end)
        return sorted(end_pp_seq_numbers)

    def processStashedCheckpoints(self, key):
        # Remove all checkpoints from previous views if any
        self._remove_stashed_checkpoints(till_3pc_key=(self.viewNo, 0))

        if key not in self.stashedRecvdCheckpoints.get(self.viewNo, {}):
            self.logger.trace("{} have no stashed checkpoints for {}")
            return 0

        # Get a snapshot of all the senders of stashed checkpoints for `key`
        senders = list(self.stashedRecvdCheckpoints[self.viewNo][key].keys())
        total_processed = 0
        consumed = 0

        for sender in senders:
            # Check if the checkpoint from `sender` is still in
            # `stashedRecvdCheckpoints` because it might be removed from there
            # in case own checkpoint was stabilized when we were processing
            # stashed checkpoints from previous senders in this loop
            if self.viewNo in self.stashedRecvdCheckpoints \
                    and key in self.stashedRecvdCheckpoints[self.viewNo] \
                    and sender in self.stashedRecvdCheckpoints[self.viewNo][key]:
                if self.processCheckpoint(
                        self.stashedRecvdCheckpoints[self.viewNo][key].pop(sender),
                        sender):
                    consumed += 1
                # Note that if `processCheckpoint` returned False then the
                # checkpoint from `sender` was re-stashed back to
                # `stashedRecvdCheckpoints`
                total_processed += 1

        # If we have consumed stashed checkpoints for `key` from all the
        # senders then remove entries which have become empty
        if self.viewNo in self.stashedRecvdCheckpoints \
                and key in self.stashedRecvdCheckpoints[self.viewNo] \
                and len(self.stashedRecvdCheckpoints[self.viewNo][key]) == 0:
            del self.stashedRecvdCheckpoints[self.viewNo][key]
            if len(self.stashedRecvdCheckpoints[self.viewNo]) == 0:
                del self.stashedRecvdCheckpoints[self.viewNo]

        restashed = total_processed - consumed
        self.logger.info('{} processed {} stashed checkpoints for {}, '
                         '{} of them were stashed again'.
                         format(self, total_processed, key, restashed))

        return total_processed

    def _gc(self, till3PCKey):
        self.logger.info("{} cleaning up till {}".format(self, till3PCKey))
        tpcKeys = set()
        reqKeys = set()
        for key3PC, pp in self.sentPrePrepares.items():
            if compare_3PC_keys(till3PCKey, key3PC) <= 0:
                tpcKeys.add(key3PC)
                for reqKey in pp.reqIdr:
                    reqKeys.add(reqKey)
        for key3PC, pp in self.prePrepares.items():
            if compare_3PC_keys(till3PCKey, key3PC) <= 0:
                tpcKeys.add(key3PC)
                for reqKey in pp.reqIdr:
                    reqKeys.add(reqKey)

        self.logger.trace("{} found {} 3-phase keys to clean".
                          format(self, len(tpcKeys)))
        self.logger.trace("{} found {} request keys to clean".
                          format(self, len(reqKeys)))

        to_clean_up = (
            self.sentPrePrepares,
            self.prePrepares,
            self.prepares,
            self.commits,
            self.batches,
            self.requested_pre_prepares,
            self.requested_prepares,
            self.requested_commits,
            self.pre_prepares_stashed_for_incorrect_time,
        )
        for request_key in tpcKeys:
            for coll in to_clean_up:
                coll.pop(request_key, None)

        for request_key in reqKeys:
            self.requests.free(request_key)
            self.logger.trace('{} freed request {} from previous checkpoints'
                              .format(self, request_key))

        self.compact_ordered()

        # BLS multi-sig:
        self._bls_bft_replica.gc(till3PCKey)

    def _gc_before_new_view(self):
        # Trigger GC for all batches of old view
        # Clear any checkpoints, since they are valid only in a view
        self._gc(self.last_ordered_3pc)
        self.checkpoints.clear()
        self._remove_stashed_checkpoints(till_3pc_key=(self.viewNo, 0))
        self._clear_prev_view_pre_prepares()

    def _reset_watermarks_before_new_view(self):
        # Reset any previous view watermarks since for view change to
        # successfully complete, the node must have reached the same state
        # as other nodes
        self.h = 0
        self._lastPrePrepareSeqNo = self.h

    def stashOutsideWatermarks(self, item: Tuple):
        self.stashingWhileOutsideWaterMarks.append(item)

    def processStashedMsgsForNewWaterMarks(self):
        # Items from `stashingWhileOutsideWaterMarks` can be re-enqueued back
        # to this queue, so `stashingWhileOutsideWaterMarks` might never
        # become empty during the execution of this method resulting
        # in an infinite loop. So we should consume each item only once
        # during one call of this method.
        itemsToConsume = len(self.stashingWhileOutsideWaterMarks)

        # Moreover, the watermarks may be updated again while consuming items
        # from `stashingWhileOutsideWaterMarks`. So this method may be called
        # recursively. We should stop iteration in all the outward calls of
        # this method in case we complete consuming of stashed messages in any
        # recursive call of this method.
        self.consumedAllStashedMsgs = False

        while itemsToConsume and not self.consumedAllStashedMsgs:
            item = self.stashingWhileOutsideWaterMarks.popleft()
            self.logger.debug("{} processing stashed item {} after new stable "
                              "checkpoint".format(self, item))

            if isinstance(item, tuple) and len(item) == 2:
                self.dispatchThreePhaseMsg(*item)
            else:
                self.logger.debug("{} cannot process {} "
                                  "from stashingWhileOutsideWaterMarks".format(self, item))
            itemsToConsume -= 1

        self.consumedAllStashedMsgs = True

    @property
    def firstCheckPoint(self) -> Tuple[Tuple[int, int], CheckpointState]:
        if not self.checkpoints:
            return None
        else:
            return self.checkpoints.peekitem(0)

    @property
    def lastCheckPoint(self) -> Tuple[Tuple[int, int], CheckpointState]:
        if not self.checkpoints:
            return None
        else:
            return self.checkpoints.peekitem(-1)

    def isPpSeqNoStable(self, ppSeqNo):
        """
        :param ppSeqNo:
        :return: True if ppSeqNo is less than or equal to last stable
        checkpoint, false otherwise
        """
        ck = self.firstCheckPoint
        if ck:
            _, ckState = ck
            return ckState.isStable and ckState.seqNo >= ppSeqNo
        else:
            return False

    def has_already_ordered(self, view_no, pp_seq_no):
        return compare_3PC_keys((view_no, pp_seq_no),
                                self.last_ordered_3pc) >= 0

    def isPpSeqNoBetweenWaterMarks(self, ppSeqNo: int):
        return self.h < ppSeqNo <= self.H

    def addToOrdered(self, view_no: int, pp_seq_no: int):
        self.ordered.add((view_no, pp_seq_no))
        self.last_ordered_3pc = (view_no, pp_seq_no)

        self.requested_pre_prepares.pop((view_no, pp_seq_no), None)
        self.requested_prepares.pop((view_no, pp_seq_no), None)
        self.requested_commits.pop((view_no, pp_seq_no), None)

    def compact_ordered(self):
        min_allowed_view_no = self.viewNo - 1
        i = 0
        for view_no, _ in self.ordered:
            if view_no >= min_allowed_view_no:
                break
            i += 1
        self.ordered = self.ordered[i:]

    def enqueue_pre_prepare(self, ppMsg: PrePrepare, sender: str,
                            nonFinReqs: Set = None):
        if nonFinReqs:
            self.logger.debug("Queueing pre-prepares due to unavailability of finalised "
                              "requests. PrePrepare {} from {}".format(ppMsg, sender))
            self.prePreparesPendingFinReqs.append((ppMsg, sender, nonFinReqs))
        else:
            # Possible exploit, an malicious party can send an invalid
            # pre-prepare and over-write the correct one?
            self.logger.debug("Queueing pre-prepares due to unavailability of previous pre-prepares. {} from {}".
                              format(ppMsg, sender))
            self.prePreparesPendingPrevPP[ppMsg.viewNo, ppMsg.ppSeqNo] = (
                ppMsg, sender)

    def dequeue_pre_prepares(self):
        """
        Dequeue any received PRE-PREPAREs that did not have finalized requests
        or the replica was missing any PRE-PREPAREs before it
        :return:
        """
        ppsReady = []
        # Check if any requests have become finalised belonging to any stashed
        # PRE-PREPAREs.
        for i, (pp, sender, reqIds) in enumerate(
                self.prePreparesPendingFinReqs):
            finalised = set()
            for r in reqIds:
                if self.requests.is_finalised(r):
                    finalised.add(r)
            diff = reqIds.difference(finalised)
            # All requests become finalised
            if not diff:
                ppsReady.append(i)
            self.prePreparesPendingFinReqs[i] = (pp, sender, diff)

        for i in sorted(ppsReady, reverse=True):
            pp, sender, _ = self.prePreparesPendingFinReqs.pop(i)
            self.prePreparesPendingPrevPP[pp.viewNo, pp.ppSeqNo] = (pp, sender)

        r = 0
        while self.prePreparesPendingPrevPP and self.__is_next_pre_prepare(
                *self.prePreparesPendingPrevPP.iloc[0]):
            _, (pp, sender) = self.prePreparesPendingPrevPP.popitem(last=False)
            if not self.can_pp_seq_no_be_in_view(pp.viewNo, pp.ppSeqNo):
                self.discard(pp, "Pre-Prepare from a previous view",
                             self.logger.debug)
                continue
            self.threePhaseRouter.handleSync((pp, sender))
            r += 1
        return r

    def enqueue_prepare(self, pMsg: Prepare, sender: str):
        self.logger.debug("{} queueing prepare due to unavailability of PRE-PREPARE. "
                          "Prepare {} from {}".format(self, pMsg, sender))
        key = (pMsg.viewNo, pMsg.ppSeqNo)
        if key not in self.preparesWaitingForPrePrepare:
            self.preparesWaitingForPrePrepare[key] = deque()
        self.preparesWaitingForPrePrepare[key].append((pMsg, sender))
        if key not in self.pre_prepares_stashed_for_incorrect_time:
            if self.isMaster or self.last_ordered_3pc[1] != 0:
                self._request_pre_prepare_for_prepare(key)
        else:
            self._process_stashed_pre_prepare_for_time_if_possible(key)

    def dequeue_prepares(self, viewNo: int, ppSeqNo: int):
        key = (viewNo, ppSeqNo)
        if key in self.preparesWaitingForPrePrepare:
            i = 0
            # Keys of pending prepares that will be processed below
            while self.preparesWaitingForPrePrepare[key]:
                prepare, sender = self.preparesWaitingForPrePrepare[
                    key].popleft()
                self.logger.debug("{} popping stashed PREPARE{}".format(self, key))
                self.threePhaseRouter.handleSync((prepare, sender))
                i += 1
            self.preparesWaitingForPrePrepare.pop(key)
            self.logger.debug("{} processed {} PREPAREs waiting for PRE-PREPARE for"
                              " view no {} and seq no {}".format(self, i, viewNo, ppSeqNo))

    def enqueue_commit(self, request: Commit, sender: str):
        self.logger.debug("Queueing commit due to unavailability of PREPARE. "
                          "Request {} from {}".format(request, sender))
        key = (request.viewNo, request.ppSeqNo)
        if key not in self.commitsWaitingForPrepare:
            self.commitsWaitingForPrepare[key] = deque()
        self.commitsWaitingForPrepare[key].append((request, sender))

    def dequeue_commits(self, viewNo: int, ppSeqNo: int):
        key = (viewNo, ppSeqNo)
        if key in self.commitsWaitingForPrepare:
            if not self.has_prepared(key):
                self.logger.debug('{} has not prepared {}, will dequeue the '
                                  'COMMITs later'.format(self, key))
                return
            i = 0
            # Keys of pending prepares that will be processed below
            while self.commitsWaitingForPrepare[key]:
                commit, sender = self.commitsWaitingForPrepare[
                    key].popleft()
                self.logger.debug("{} popping stashed COMMIT{}".format(self, key))
                self.threePhaseRouter.handleSync((commit, sender))

                i += 1
            self.commitsWaitingForPrepare.pop(key)
            self.logger.debug("{} processed {} COMMITs waiting for PREPARE for"
                              " view no {} and seq no {}".format(self, i, viewNo, ppSeqNo))

    def getDigestFor3PhaseKey(self, key: ThreePhaseKey) -> Optional[str]:
        reqKey = self.getReqKeyFrom3PhaseKey(key)
        digest = self.requests.digest(reqKey)
        if not digest:
            self.logger.debug("{} could not find digest in sent or received "
                              "PRE-PREPAREs or PREPAREs for 3 phase key {} and req "
                              "key {}".format(self, key, reqKey))
            return None
        else:
            return digest

    def getReqKeyFrom3PhaseKey(self, key: ThreePhaseKey):
        reqKey = None
        if key in self.sentPrePrepares:
            reqKey = self.sentPrePrepares[key][0]
        elif key in self.prePrepares:
            reqKey = self.prePrepares[key][0]
        elif key in self.prepares:
            reqKey = self.prepares[key][0]
        else:
            self.logger.debug("Could not find request key for 3 phase key {}".format(key))
        return reqKey

    def can_pp_seq_no_be_in_view(self, view_no, pp_seq_no):
        """
        Checks if the `pp_seq_no` could have been in view `view_no`. It will
        return False when the `pp_seq_no` belongs to a later view than
        `view_no` else will return True
        :return:
        """
        if view_no > self.viewNo:
            raise PlenumValueError(
                'view_no', view_no,
                "<= current view_no {}".format(self.viewNo),
                prefix=self
            )

        return view_no == self.viewNo or (
            view_no < self.viewNo and self.last_prepared_before_view_change and compare_3PC_keys(
                (view_no, pp_seq_no), self.last_prepared_before_view_change) >= 0)

    def _request_missing_three_phase_messages(self, view_no: int, seq_frm: int, seq_to: int) -> None:
        for pp_seq_no in range(seq_frm, seq_to + 1):
            key = (view_no, pp_seq_no)
            self._request_pre_prepare(key)
            self._request_prepare(key)
            self._request_commit(key)

    def _request_three_phase_msg(self, three_pc_key: Tuple[int, int],
                                 stash: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]],
                                 msg_type: str,
                                 recipients: Optional[List[str]] = None,
                                 stash_data: Optional[Tuple[str, str, str]] = None) -> bool:
        if three_pc_key in stash:
            self.logger.debug('{} not requesting {} since already '
                              'requested for {}'.format(self, msg_type, three_pc_key))
            return False

        # TODO: Using a timer to retry would be a better thing to do
        self.logger.trace('{} requesting {} for {} from {}'.format(
            self, msg_type, three_pc_key, recipients))
        # An optimisation can be to request PRE-PREPARE from f+1 or
        # f+x (f+x<2f) nodes only rather than 2f since only 1 correct
        # PRE-PREPARE is needed.
        self.node.request_msg(msg_type, {f.INST_ID.nm: self.instId,
                                         f.VIEW_NO.nm: three_pc_key[0],
                                         f.PP_SEQ_NO.nm: three_pc_key[1]},
                              recipients)

        stash[three_pc_key] = stash_data
        return True

    def _request_pre_prepare(self, three_pc_key: Tuple[int, int],
                             stash_data: Optional[Tuple[str, str, str]] = None) -> bool:
        """
        Request preprepare
        """
        recipients = self.primaryName
        return self._request_three_phase_msg(three_pc_key,
                                             self.requested_pre_prepares,
                                             PREPREPARE,
                                             recipients,
                                             stash_data)

    def _request_prepare(self, three_pc_key: Tuple[int, int],
                         recipients: List[str] = None,
                         stash_data: Optional[Tuple[str, str, str]] = None) -> bool:
        """
        Request preprepare
        """
        if recipients is None:
            recipients = self.node.nodestack.connecteds.copy()
            primaryName = self.primaryName[:self.primaryName.rfind(":")]
            recipients.discard(primaryName)
        return self._request_three_phase_msg(three_pc_key, self.requested_prepares, PREPARE, recipients, stash_data)

    def _request_commit(self, three_pc_key: Tuple[int, int],
                        recipients: List[str] = None) -> bool:
        """
        Request commit
        """
        return self._request_three_phase_msg(three_pc_key, self.requested_commits, COMMIT, recipients)

    def _request_pre_prepare_for_prepare(self, three_pc_key) -> bool:
        """
        Check if has an acceptable PRE_PREPARE already stashed, if not then
        check count of PREPAREs, make sure >f consistent PREPAREs are found,
        store the acceptable PREPARE state (digest, roots) for verification of
        the received PRE-PREPARE
        """

        if three_pc_key in self.prePreparesPendingPrevPP:
            self.logger.debug('{} not requesting a PRE-PREPARE since already found '
                              'stashed for {}'.format(self, three_pc_key))
            return False

        if len(
                self.preparesWaitingForPrePrepare[three_pc_key]) < self.quorums.prepare.value:
            self.logger.debug(
                '{} not requesting a PRE-PREPARE because does not have'
                ' sufficient PREPAREs for {}'.format(
                    self, three_pc_key))
            return False

        digest, state_root, txn_root, _ = \
            self.get_acceptable_stashed_prepare_state(three_pc_key)

        # Choose a better data structure for `prePreparesPendingFinReqs`
        pre_prepares = [pp for pp, _, _ in self.prePreparesPendingFinReqs
                        if (pp.viewNo, pp.ppSeqNo) == three_pc_key]
        if pre_prepares:
            if [pp for pp in pre_prepares if (
                pp.digest, pp.stateRootHash, pp.txnRootHash) == (
                    digest, state_root, txn_root)]:
                self.logger.debug('{} not requesting a PRE-PREPARE since already '
                                  'found stashed for {}'.format(self, three_pc_key))
                return False

        self._request_pre_prepare(three_pc_key,
                                  stash_data=(digest, state_root, txn_root))
        return True

    def get_acceptable_stashed_prepare_state(self, three_pc_key):
        prepares = {s: (m.digest, m.stateRootHash, m.txnRootHash) for m, s in
                    self.preparesWaitingForPrePrepare[three_pc_key]}
        acceptable, freq = mostCommonElement(prepares.values())
        return (*acceptable, {s for s, state in prepares.items()
                              if state == acceptable})

    def _process_requested_three_phase_msg(self, msg: object,
                                           sender: List[str],
                                           stash: Dict[Tuple[int, int], Optional[Tuple[str, str, str]]],
                                           get_saved: Optional[Callable[[int, int], Optional[MessageBase]]] = None):
        if msg is None:
            self.logger.debug('{} received null from {}'.format(self, sender))
            return
        key = (msg.viewNo, msg.ppSeqNo)
        self.logger.debug('{} received requested msg ({}) from {}'.format(self, key, sender))

        if key not in stash:
            self.logger.debug('{} had either not requested this msg or already '
                              'received the msg for {}'.format(self, key))
            return
        if self.has_already_ordered(*key):
            self.logger.debug(
                '{} has already ordered msg ({})'.format(self, key))
            return
        if get_saved and get_saved(*key):
            self.logger.debug(
                '{} has already received msg ({})'.format(self, key))
            return
        # There still might be stashed msg but not checking that
        # it is expensive, also reception of msgs is idempotent
        stashed_data = stash[key]
        curr_data = (msg.digest, msg.stateRootHash, msg.txnRootHash) \
            if isinstance(msg, PrePrepare) or isinstance(msg, Prepare) \
            else None
        if stashed_data is None or curr_data == stashed_data:
            return self.processThreePhaseMsg(msg, sender)

        self.discard(msg, reason='{} does not have expected state {}'.
                     format(THREE_PC_PREFIX, stashed_data),
                     logMethod=self.logger.warning)

    def process_requested_pre_prepare(self, pp: PrePrepare, sender: str):
        return self._process_requested_three_phase_msg(pp, sender, self.requested_pre_prepares, self.getPrePrepare)

    def process_requested_prepare(self, prepare: Prepare, sender: str):
        return self._process_requested_three_phase_msg(prepare, sender, self.requested_prepares)

    def process_requested_commit(self, commit: Commit, sender: str):
        return self._process_requested_three_phase_msg(commit, sender, self.requested_commits)

    def is_pre_prepare_time_correct(self, pp: PrePrepare) -> bool:
        """
        Check if this PRE-PREPARE is not older than (not checking for greater
        than since batches maybe sent in less than 1 second) last PRE-PREPARE
        and in a sufficient range of local clock's UTC time.
        :param pp:
        :return:
        """
        return ((self.last_accepted_pre_prepare_time is None or
                 pp.ppTime >= self.last_accepted_pre_prepare_time) and
                (abs(pp.ppTime - self.utc_epoch) <= self.config.ACCEPTABLE_DEVIATION_PREPREPARE_SECS))

    def is_pre_prepare_time_acceptable(self, pp: PrePrepare) -> bool:
        """
        Returns True or False depending on the whether the time in PRE-PREPARE
        is acceptable. Can return True if time is not acceptable but sufficient
        PREPAREs are found to support the PRE-PREPARE
        :param pp:
        :return:
        """
        key = (pp.viewNo, pp.ppSeqNo)
        if key in self.requested_pre_prepares:
            # Special case for requested PrePrepares
            return True
        correct = self.is_pre_prepare_time_correct(pp)
        if not correct:
            self.logger.warning('{} found {} to have incorrect time.'.format(self, pp))
            if key in self.pre_prepares_stashed_for_incorrect_time and \
                    self.pre_prepares_stashed_for_incorrect_time[key][-1]:
                self.logger.debug('{} marking time as correct for {}'.format(self, pp))
                correct = True
        return correct

    def _process_stashed_pre_prepare_for_time_if_possible(
            self, key: Tuple[int, int]):
        """
        Check if any PRE-PREPAREs that were stashed since their time was not
        acceptable, can now be accepted since enough PREPAREs are received
        """
        self.logger.debug('{} going to process stashed PRE-PREPAREs with '
                          'incorrect times'.format(self))
        q = self.quorums.f
        if len(self.preparesWaitingForPrePrepare[key]) > q:
            times = [pr.ppTime for (pr, _) in
                     self.preparesWaitingForPrePrepare[key]]
            most_common_time, freq = mostCommonElement(times)
            if self.quorums.timestamp.is_reached(freq):
                self.logger.debug('{} found sufficient PREPAREs for the '
                                  'PRE-PREPARE{}'.format(self, key))
                stashed_pp = self.pre_prepares_stashed_for_incorrect_time
                pp, sender, done = stashed_pp[key]
                if done:
                    self.logger.debug('{} already processed PRE-PREPARE{}'.format(self, key))
                    return True
                # True is set since that will indicate to `is_pre_prepare_time_acceptable`
                # that sufficient PREPAREs are received
                stashed_pp[key] = (pp, sender, True)
                self.threePhaseRouter.handleSync((pp, sender))
                return True
        return False

    # @property
    # def threePhaseState(self):
    #     # TODO: This method is incomplete
    #     # Gets the current stable and unstable checkpoints and creates digest
    #     # of unstable checkpoints
    #     if self.checkpoints:
    #         pass
    #     else:
    #         state = []
    #     return ThreePCState(self.instId, state)

    def process3PhaseState(self, msg: ThreePCState, sender: str):
        # TODO: This is not complete
        pass

    def send(self, msg, stat=None) -> None:
        """
        Send a message to the node on which this replica resides.

        :param stat:
        :param rid: remote id of one recipient (sends to all recipients if None)
        :param msg: the message to send
        """
        self.logger.trace("{} sending {}".format(self, msg.__class__.__name__),
                          extra={"cli": True, "tags": ['sending']})
        self.logger.trace("{} sending {}".format(self, msg))
        if stat:
            self.stats.inc(stat)
        self.outBox.append(msg)

    def revert_unordered_batches(self):
        """
        Revert changes to ledger (uncommitted) and state made by any requests
        that have not been ordered.
        """
        i = 0
        for key in sorted(self.batches.keys(), reverse=True):
            if compare_3PC_keys(self.last_ordered_3pc, key) > 0:
                ledger_id, count, _, prevStateRoot = self.batches.pop(key)
                self.logger.debug('{} reverting 3PC key {}'.format(self, key))
                self.revert(ledger_id, prevStateRoot, count)
                i += 1
            else:
                break
        return i

    def update_watermark_from_3pc(self):
        if (self.last_ordered_3pc is not None) and (self.last_ordered_3pc[0] == self.viewNo):
            self.logger.info("update_watermark_from_3pc to {}".format(self.last_ordered_3pc))
            self.h = self.last_ordered_3pc[1]
        else:
            self.logger.info("try to update_watermark_from_3pc but last_ordered_3pc is None")

    def caught_up_till_3pc(self, last_caught_up_3PC):
        self.last_ordered_3pc = last_caught_up_3PC
        self._remove_till_caught_up_3pc(last_caught_up_3PC)
        self._remove_ordered_from_queue(last_caught_up_3PC)
        self.checkpoints.clear()
        self._remove_stashed_checkpoints(till_3pc_key=last_caught_up_3PC)
        self.update_watermark_from_3pc()

    def catchup_clear_for_backup(self):
        if not self.isPrimary:
            self.last_ordered_3pc = (self.viewNo, 0)
            self.batches.clear()
            self.sentPrePrepares.clear()
            self.prePrepares.clear()
            self.prepares.clear()
            self.commits.clear()
            self.outBox.clear()
            self.checkpoints.clear()
            self._remove_stashed_checkpoints()
            self.h = 0
            self.H = sys.maxsize

    def _remove_till_caught_up_3pc(self, last_caught_up_3PC):
        """
        Remove any 3 phase messages till the last ordered key and also remove
        any corresponding request keys
        """
        outdated_pre_prepares = {}
        for key, pp in self.prePrepares.items():
            if compare_3PC_keys(key, last_caught_up_3PC) >= 0:
                outdated_pre_prepares[key] = pp
        for key, pp in self.sentPrePrepares.items():
            if compare_3PC_keys(key, last_caught_up_3PC) >= 0:
                outdated_pre_prepares[key] = pp

        self.logger.trace('{} going to remove messages for {} 3PC keys'.format(
            self, len(outdated_pre_prepares)))

        for key, pp in outdated_pre_prepares.items():
            self.batches.pop(key, None)
            self.sentPrePrepares.pop(key, None)
            self.prePrepares.pop(key, None)
            self.prepares.pop(key, None)
            self.commits.pop(key, None)
            self._discard_ordered_req_keys(pp)

    def _remove_ordered_from_queue(self, last_caught_up_3PC=None):
        """
        Remove any Ordered that the replica might be sending to node which is
        less than or equal to `last_caught_up_3PC` if `last_caught_up_3PC` is
        passed else remove all ordered, needed in catchup
        """
        to_remove = []
        for i, msg in enumerate(self.outBox):
            if isinstance(msg, Ordered) and \
                    (not last_caught_up_3PC or
                     compare_3PC_keys((msg.viewNo, msg.ppSeqNo), last_caught_up_3PC) >= 0):
                to_remove.append(i)

        self.logger.trace('{} going to remove {} Ordered messages from outbox'.format(self, len(to_remove)))

        # Removing Ordered from queue but returning `Ordered` in order that
        # they should be processed.
        removed = []
        for i in reversed(to_remove):
            removed.insert(0, self.outBox[i])
            del self.outBox[i]
        return removed

    def _remove_stashed_checkpoints(self, till_3pc_key=None):
        """
        Remove stashed received checkpoints up to `till_3pc_key` if provided,
        otherwise remove all stashed received checkpoints
        """
        if till_3pc_key is None:
            self.stashedRecvdCheckpoints.clear()
            self.logger.info('{} removing all stashed checkpoints'.format(self))
            return

        for view_no in list(self.stashedRecvdCheckpoints.keys()):

            if view_no < till_3pc_key[0]:
                self.logger.info('{} removing stashed checkpoints for view {}'.format(self, view_no))
                del self.stashedRecvdCheckpoints[view_no]

            elif view_no == till_3pc_key[0]:
                for (s, e) in list(self.stashedRecvdCheckpoints[view_no].keys()):
                    if e <= till_3pc_key[1]:
                        self.logger.info('{} removing stashed checkpoints: '
                                         'viewNo={}, seqNoStart={}, seqNoEnd={}'.
                                         format(self, view_no, s, e))
                        del self.stashedRecvdCheckpoints[view_no][(s, e)]
                if len(self.stashedRecvdCheckpoints[view_no]) == 0:
                    del self.stashedRecvdCheckpoints[view_no]

    def _get_last_timestamp_from_state(self, ledger_id):
        if ledger_id == DOMAIN_LEDGER_ID:
            req_handler = self.node.ledger_to_req_handler.get(ledger_id)
            if req_handler.ts_store:
                last_timestamp = req_handler.ts_store.get_last_key()
                if last_timestamp:
                    last_timestamp = int(last_timestamp.decode())
                    self.logger.debug("Last ordered timestamp from store is : {}"
                                      "".format(last_timestamp))
                    return last_timestamp
        return None
