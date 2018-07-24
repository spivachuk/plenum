import json
import os
import time
from binascii import unhexlify
from collections import deque
from contextlib import closing
from functools import partial
from typing import Dict, Any, Mapping, Iterable, List, Optional, Set, Tuple, Callable

from intervaltree import IntervalTree

from common.exceptions import LogicError
from crypto.bls.bls_key_manager import LoadBLSKeyError
from plenum.server.inconsistency_watchers import NetworkInconsistencyWatcher
from state.pruning_state import PruningState
from state.state import State
from storage.helper import initKeyValueStorage, initHashStore, initKeyValueStorageIntKeys
from storage.state_ts_store import StateTsDbStorage
from stp_core.common.log import getlogger
from stp_core.crypto.signer import Signer
from stp_core.network.exceptions import RemoteNotFound
from stp_core.network.network_interface import NetworkInterface
from stp_core.types import HA
from stp_zmq.zstack import ZStack
from ledger.compact_merkle_tree import CompactMerkleTree
from ledger.genesis_txn.genesis_txn_initiator_from_file import GenesisTxnInitiatorFromFile
from ledger.hash_stores.hash_store import HashStore

from plenum.common.config_util import getConfig
from plenum.common.constants import POOL_LEDGER_ID, DOMAIN_LEDGER_ID, \
    CLIENT_BLACKLISTER_SUFFIX, CONFIG_LEDGER_ID, \
    NODE_BLACKLISTER_SUFFIX, NODE_PRIMARY_STORAGE_SUFFIX, \
    TXN_TYPE, LEDGER_STATUS, \
    CLIENT_STACK_SUFFIX, PRIMARY_SELECTION_PREFIX, VIEW_CHANGE_PREFIX, \
    OP_FIELD_NAME, CATCH_UP_PREFIX, NYM, \
    GET_TXN, DATA, VERKEY, \
    TARGET_NYM, ROLE, STEWARD, TRUSTEE, ALIAS, \
    NODE_IP, BLS_PREFIX, NodeHooks, LedgerState, CURRENT_PROTOCOL_VERSION
from plenum.common.exceptions import SuspiciousNode, SuspiciousClient, \
    MissingNodeOp, InvalidNodeOp, InvalidNodeMsg, InvalidClientMsgType, \
    InvalidClientRequest, BaseExc, \
    InvalidClientMessageException, KeysNotFoundException as REx, BlowUp
from plenum.common.has_file_storage import HasFileStorage
from plenum.common.hook_manager import HookManager
from plenum.common.keygen_utils import areKeysSetup
from plenum.common.ledger import Ledger
from plenum.common.ledger_manager import LedgerManager
from plenum.common.message_processor import MessageProcessor
from plenum.common.messages.node_message_factory import node_message_factory
from plenum.common.messages.node_messages import Nomination, Batch, Reelection, \
    Primary, RequestAck, RequestNack, Reject, PoolLedgerTxns, Ordered, \
    Propagate, PrePrepare, Prepare, Commit, Checkpoint, ThreePCState, Reply, InstanceChange, LedgerStatus, \
    ConsistencyProof, CatchupReq, CatchupRep, ViewChangeDone, \
    CurrentState, MessageReq, MessageRep, ThreePhaseType, BatchCommitted, \
    ObservedData, FutureViewChangeDone
from plenum.common.motor import Motor
from plenum.common.plugin_helper import loadPlugins
from plenum.common.request import Request, SafeRequest
from plenum.common.roles import Roles
from plenum.common.signer_simple import SimpleSigner
from plenum.common.stacks import nodeStackClass, clientStackClass
from plenum.common.startable import Status, Mode
from plenum.common.txn_util import idr_from_req_data, get_req_id, \
    get_seq_no, get_type, get_payload_data, \
    get_txn_time, get_digest
from plenum.common.types import PLUGIN_TYPE_VERIFICATION, \
    PLUGIN_TYPE_PROCESSING, OPERATION, f
from plenum.common.util import friendlyEx, getMaxFailures, pop_keys, \
    compare_3PC_keys, get_utc_epoch
from plenum.common.verifier import DidVerifier
from plenum.common.config_helper import PNodeConfigHelper

from plenum.persistence.req_id_to_txn import ReqIdrToTxn
from plenum.persistence.storage import Storage, initStorage
from plenum.bls.bls_bft_factory import create_default_bls_bft_factory
from plenum.bls.bls_crypto_factory import create_default_bls_crypto_factory
from plenum.recorder.recorder import add_start_time, add_stop_time

from plenum.client.wallet import Wallet

from plenum.server.ledger_req_handler import LedgerRequestHandler
from plenum.server.action_req_handler import ActionReqHandler
from plenum.server.blacklister import Blacklister
from plenum.server.blacklister import SimpleBlacklister
from plenum.server.client_authn import ClientAuthNr, SimpleAuthNr, CoreAuthNr
from plenum.server.config_req_handler import ConfigReqHandler
from plenum.server.domain_req_handler import DomainRequestHandler
from plenum.server.has_action_queue import HasActionQueue
from plenum.server.instances import Instances
from plenum.server.message_req_processor import MessageReqProcessor
from plenum.server.monitor import Monitor
from plenum.server.notifier_plugin_manager import notifierPluginTriggerEvents, \
    PluginManager
from plenum.server.observer.observable import Observable
from plenum.server.observer.observer_node import NodeObserver
from plenum.server.observer.observer_sync_policy import ObserverSyncPolicyType
from plenum.server.plugin.has_plugin_loader_helper import PluginLoaderHelper
from plenum.server.pool_manager import HasPoolManager, TxnPoolManager
from plenum.server.primary_decider import PrimaryDecider
from plenum.server.primary_selector import PrimarySelector
from plenum.server.propagator import Propagator
from plenum.server.quorums import Quorums
from plenum.server.replicas import Replicas
from plenum.server.req_authenticator import ReqAuthenticator
from plenum.server.req_handler import RequestHandler
from plenum.server.router import Router
from plenum.server.suspicion_codes import Suspicions
from plenum.server.validator_info_tool import ValidatorNodeInfoTool
from plenum.server.view_change.view_changer import ViewChanger

pluginManager = PluginManager()
logger = getlogger()


class Node(HasActionQueue, Motor, Propagator, MessageProcessor, HasFileStorage,
           HasPoolManager, PluginLoaderHelper, MessageReqProcessor, HookManager):
    """
    A node in a plenum system.
    """

    suspicions = {s.code: s.reason for s in Suspicions.get_list()}
    keygenScript = "init_plenum_keys"
    client_request_class = SafeRequest
    _info_tool_class = ValidatorNodeInfoTool
    # The order of ledger id in the following list determines the order in
    # which those ledgers will be synced. Think carefully before changing the
    # order.
    ledger_ids = [POOL_LEDGER_ID, CONFIG_LEDGER_ID, DOMAIN_LEDGER_ID]
    _wallet_class = Wallet

    def __init__(self,
                 name: str,
                 clientAuthNr: ClientAuthNr=None,
                 ha: HA=None,
                 cliname: str=None,
                 cliha: HA=None,
                 config_helper=None,
                 ledger_dir: str = None,
                 keys_dir: str = None,
                 genesis_dir: str = None,
                 plugins_dir: str = None,
                 node_info_dir: str = None,
                 view_changer: ViewChanger = None,
                 primaryDecider: PrimaryDecider = None,
                 pluginPaths: Iterable[str] = None,
                 storage: Storage = None,
                 config=None,
                 seed=None):
        """
        Create a new node.

        :param clientAuthNr: client authenticator implementation to be used
        :param primaryDecider: the mechanism to be used to decide the primary
        of a protocol instance
        """
        self.created = time.time()
        self.name = name
        self.config = config or getConfig()

        self.config_helper = config_helper or PNodeConfigHelper(self.name, self.config)

        self.ledger_dir = ledger_dir or self.config_helper.ledger_dir
        self.keys_dir = keys_dir or self.config_helper.keys_dir
        self.genesis_dir = genesis_dir or self.config_helper.genesis_dir
        self.plugins_dir = plugins_dir or self.config_helper.plugins_dir
        self.node_info_dir = node_info_dir or self.config_helper.node_info_dir

        if self.config.STACK_COMPANION == 1:
            add_start_time(self.ledger_dir, self.utc_epoch())

        self._view_change_timeout = self.config.VIEW_CHANGE_TIMEOUT

        HasFileStorage.__init__(self, self.ledger_dir)
        self.ensureKeysAreSetup()
        self.opVerifiers = self.getPluginsByType(pluginPaths,
                                                 PLUGIN_TYPE_VERIFICATION)
        self.reqProcessors = self.getPluginsByType(pluginPaths,
                                                   PLUGIN_TYPE_PROCESSING)

        self.ledger_to_req_handler = {}  # type: Dict[int, RequestHandler]
        self.txn_type_to_req_handler = {}  # type: Dict[str, RequestHandler]
        self.txn_type_to_ledger_id = {}  # type: Dict[str, int]
        self.requestExecuter = {}   # type: Dict[int, Callable]

        Motor.__init__(self)

        self.states = {}  # type: Dict[int, State]

        self.primaryStorage = storage or self.getPrimaryStorage()

        # This is storage for storing map: timestamp/state.headHash
        # Now it used in domainLedger
        self.stateTsDbStorage = None

        self.register_state(DOMAIN_LEDGER_ID, self.loadDomainState())

        self.initPoolManager(ha, cliname, cliha)

        # init BLS after pool manager!
        # init before domain req handler!
        self.bls_bft = self._create_bls_bft()

        self.register_req_handler(self.getDomainReqHandler(), DOMAIN_LEDGER_ID)
        self.register_executer(DOMAIN_LEDGER_ID, self.executeDomainTxns)

        self.initDomainState()

        self.clientAuthNr = clientAuthNr or self.defaultAuthNr()

        self.addGenesisNyms()

        self.mode = None  # type: Optional[Mode]
        self.register_req_handler(self.poolManager.reqHandler, POOL_LEDGER_ID)

        self.nodeReg = self.poolManager.nodeReg

        kwargs = dict(stackParams=self.poolManager.nstack,
                      msgHandler=self.handleOneNodeMsg, registry=self.nodeReg)
        cls = self.nodeStackClass
        kwargs.update(seed=seed)
        # noinspection PyCallingNonCallable
        self.nodestack = cls(**kwargs)
        self.nodestack.onConnsChanged = self.onConnsChanged

        kwargs = dict(
            stackParams=self.poolManager.cstack,
            msgHandler=self.handleOneClientMsg,
            # TODO, Reject is used when dynamic validation fails, use Reqnack
            msgRejectHandler=self.reject_client_msg_handler)
        cls = self.clientStackClass
        kwargs.update(seed=seed)

        # noinspection PyCallingNonCallable
        self.clientstack = cls(**kwargs)

        self.cliNodeReg = self.poolManager.cliNodeReg

        HasActionQueue.__init__(self)

        Propagator.__init__(self)

        MessageReqProcessor.__init__(self)

        self.view_changer = view_changer
        self.primaryDecider = primaryDecider

        self.nodeInBox = deque()
        self.clientInBox = deque()

        # 3PC state consistency watchdog based on network events
        self.network_i3pc_watcher = NetworkInconsistencyWatcher(self.on_inconsistent_3pc_state_from_network)

        self.setPoolParams()

        self.network_i3pc_watcher.connect(self.name)

        self.clientBlacklister = SimpleBlacklister(
            self.name + CLIENT_BLACKLISTER_SUFFIX)  # type: Blacklister

        self.nodeBlacklister = SimpleBlacklister(
            self.name + NODE_BLACKLISTER_SUFFIX)  # type: Blacklister

        self.nodeInfo = {
            'data': {}
        }

        self._view_changer = None  # type: ViewChanger
        self._elector = None  # type: PrimaryDecider

        self.instances = Instances()
        # QUESTION: Why does the monitor need blacklister?
        self.monitor = Monitor(self.name,
                               Delta=self.config.DELTA,
                               Lambda=self.config.LAMBDA,
                               Omega=self.config.OMEGA,
                               instances=self.instances,
                               nodestack=self.nodestack,
                               blacklister=self.nodeBlacklister,
                               nodeInfo=self.nodeInfo,
                               notifierEventTriggeringConfig=self.config.notifierEventTriggeringConfig,
                               pluginPaths=pluginPaths,
                               notifierEventsEnabled=self.config.SpikeEventsEnabled)

        self.replicas = self.create_replicas()

        # Any messages that are intended for protocol instances not created.
        # Helps in cases where a new protocol instance have been added by a
        # majority of nodes due to joining of a new node, but some slow nodes
        # are not aware of it. Key is instance id and value is a deque
        # TODO is it possible for messages with current view number?
        self.msgsForFutureReplicas = {}

        # Requests that are to be given to the view_changer by the node
        self.msgsToViewChanger = deque()

        if self.poolLedger:
            self.register_state(POOL_LEDGER_ID, self.poolManager.state)

        self.ledgerManager = self.get_new_ledger_manager()

        # do it after all states and BLS stores are created
        self.adjustReplicas()

        self.perfCheckFreq = self.config.PerfCheckFreq
        self.nodeRequestSpikeMonitorData = {
            'value': 0,
            'cnt': 0,
            'accum': 0
        }

        self.startRepeating(self.checkPerformance, self.perfCheckFreq)

        self.startRepeating(self.checkNodeRequestSpike,
                            self.config
                            .notifierEventTriggeringConfig[
                                'nodeRequestSpike']['freq'])

        # BE CAREFUL HERE
        # This controls which message types are excluded from signature
        # verification. Expressly prohibited from being in this is
        # ClientRequest and Propagation, which both require client
        # signature verification
        self.authnWhitelist = (
            Nomination,
            Primary,
            Reelection,
            Batch,
            ViewChangeDone,
            PrePrepare,
            Prepare,
            Checkpoint,
            Commit,
            InstanceChange,
            LedgerStatus,
            ConsistencyProof,
            CatchupReq,
            CatchupRep,
            ThreePCState,
            MessageReq,
            MessageRep,
            CurrentState,
            ObservedData
        )

        # Map of request identifier, request id to client name. Used for
        # dispatching the processed requests to the correct client remote
        self.requestSender = {}  # Dict[Tuple[str, int], str]

        # CurrentState
        self.nodeMsgRouter = Router(
            (Propagate, self.processPropagate),
            (InstanceChange, self.sendToViewChanger),
            (ViewChangeDone, self.sendToViewChanger),
            (MessageReq, self.process_message_req),
            (MessageRep, self.process_message_rep),
            (PrePrepare, self.sendToReplica),
            (Prepare, self.sendToReplica),
            (Commit, self.sendToReplica),
            (Checkpoint, self.sendToReplica),
            (ThreePCState, self.sendToReplica),
            (LedgerStatus, self.ledgerManager.processLedgerStatus),
            (ConsistencyProof, self.ledgerManager.processConsistencyProof),
            (CatchupReq, self.ledgerManager.processCatchupReq),
            (CatchupRep, self.ledgerManager.processCatchupRep),
            (CurrentState, self.process_current_state_message),
            (ObservedData, self.send_to_observer)
        )

        self.clientMsgRouter = Router(
            (Request, self.processRequest),
            (LedgerStatus, self.ledgerManager.processLedgerStatus),
            (CatchupReq, self.ledgerManager.processCatchupReq),
        )

        # Ordered requests received from replicas while the node was not
        # participating
        self.stashedOrderedReqs = deque()

        # Set of (identifier, reqId) of all transactions that were received
        # while catching up. Used to detect overlap between stashed requests
        # and received replies while catching up.
        # self.reqsFromCatchupReplies = set()

        # Any messages that are intended for view numbers higher than the
        # current view.
        self.msgsForFutureViews = {}

        # Need to keep track of the time when lost connection with primary,
        # help in voting for/against a view change. It is supposed that a primary
        # is lost until the primary is connected.
        self.lost_primary_at = time.perf_counter()

        plugins_to_load = self.config.PluginsToLoad if hasattr(self.config, "PluginsToLoad") else None
        tp = loadPlugins(self.plugins_dir, plugins_to_load)
        logger.info("total plugins loaded in node: {}".format(tp))
        # TODO: this is already happening in `start`, why here then?
        self.logNodeInfo()
        self._wallet = None
        self.seqNoDB = self.loadSeqNoDB()

        # Stores the 3 phase keys for last `ProcessedBatchMapsToKeep` batches,
        # the key is the ledger id and value is an interval tree with each
        # interval being the range of txns and value being the 3 phase key of
        # the batch in which those transactions were included. The txn range is
        # exclusive of last seq no so to store txns from 1 to 100 add a range
        # of `1:101`
        self.txn_seq_range_to_3phase_key = {}  # type: Dict[int, IntervalTree]

        # Number of rounds of catchup done during a view change.
        self.catchup_rounds_without_txns = 0
        # The start time of the catch-up during view change
        self._catch_up_start_ts = 0

        # Number of read requests the node has processed
        self.total_read_request_number = 0
        self._info_tool = self._info_tool_class(self)

        self._last_performance_check_data = {}

        self.init_config_ledger_and_req_handler()

        self.init_ledger_manager()

        HookManager.__init__(self, NodeHooks.get_all_vals())

        self._observable = Observable()
        self._observer = NodeObserver(self)

    def init_config_ledger_and_req_handler(self):
        self.configLedger = self.getConfigLedger()
        self.init_config_state()

    @property
    def viewNo(self):
        return None if self.view_changer is None else self.view_changer.view_no

    # TODO not sure that this should be allowed
    @viewNo.setter
    def viewNo(self, value):
        self.view_changer.view_no = value

    @property
    def view_change_in_progress(self):
        return (False if self.view_changer is None else
                self.view_changer.view_change_in_progress)

    def init_config_state(self):
        self.register_state(CONFIG_LEDGER_ID, self.loadConfigState())
        self.setup_config_req_handler()
        self.setup_action_req_handler()
        self.initConfigState()

    def _add_config_ledger(self):
        self.ledgerManager.addLedger(
            CONFIG_LEDGER_ID,
            self.configLedger,
            preCatchupStartClbk=self.preConfigLedgerCatchup,
            postCatchupCompleteClbk=self.postConfigLedgerCaughtUp,
            postTxnAddedToLedgerClbk=self.postTxnFromCatchupAddedToLedger)
        self.on_new_ledger_added(CONFIG_LEDGER_ID)

    def setup_config_req_handler(self):
        self.configReqHandler = self.getConfigReqHandler()
        self.register_req_handler(self.configReqHandler, CONFIG_LEDGER_ID)

    def setup_action_req_handler(self):
        self.actionReqHandler = self.get_action_req_handler()
        self.register_req_handler(self.actionReqHandler)

    def getConfigLedger(self):
        return Ledger(CompactMerkleTree(hashStore=self.getHashStore('config')),
                      dataDir=self.dataLocation,
                      fileName=self.config.configTransactionsFile,
                      ensureDurability=self.config.EnsureLedgerDurability)

    def loadConfigState(self):
        return PruningState(
            initKeyValueStorage(
                self.config.configStateStorage,
                self.dataLocation,
                self.config.configStateDbName,
                db_config=self.config.db_state_config)
        )

    def initConfigState(self):
        self.initStateFromLedger(self.states[CONFIG_LEDGER_ID],
                                 self.configLedger, self.configReqHandler)

    def getConfigReqHandler(self):
        return ConfigReqHandler(self.configLedger,
                                self.states[CONFIG_LEDGER_ID])

    def get_action_req_handler(self):
        return ActionReqHandler()

    def prePoolLedgerCatchup(self, **kwargs):
        self.mode = Mode.discovering

    def preConfigLedgerCatchup(self, **kwargs):
        self.mode = Mode.syncing

    def preDomainLedgerCatchup(self, **kwargs):
        self.mode = Mode.syncing

    def postConfigLedgerCaughtUp(self, **kwargs):
        pass

    @property
    def configLedgerStatus(self):
        return self.build_ledger_status(CONFIG_LEDGER_ID)

    def reject_client_msg_handler(self, reason, frm):
        self.transmitToClient(Reject("", "", reason), frm)

    @property
    def id(self):
        if isinstance(self.poolManager, TxnPoolManager):
            return self.poolManager.id
        return None

    @property
    def wallet(self):
        if not self._wallet:
            wallet = self._wallet_class(self.name)
            # TODO: Should use DidSigner to move away from cryptonyms
            signer = SimpleSigner(seed=unhexlify(self.nodestack.keyhex))
            wallet.addIdentifier(signer=signer)
            self._wallet = wallet
        return self._wallet

    @property
    def ledger_summary(self):
        return [li.ledger_summary for li in
                self.ledgerManager.ledgerRegistry.values()]

    @property
    def view_changer(self) -> ViewChanger:
        return self._view_changer

    @view_changer.setter
    def view_changer(self, value):
        self._view_changer = value

    @property
    def elector(self) -> PrimaryDecider:
        return self._elector

    @elector.setter
    def elector(self, value):
        self._elector = value

    # EXTERNAL EVENTS

    def on_view_change_start(self):
        """
        Notifies node about the fact that view changed to let it
        prepare for election
        """
        self.view_changer.start_view_change_ts = self.utc_epoch()

        for replica in self.replicas:
            replica.on_view_change_start()
        logger.info("{} resetting monitor stats at view change start".format(self))
        self.monitor.reset()
        self.processStashedMsgsForView(self.viewNo)

        for replica in self.replicas:
            replica.primaryName = None

        pop_keys(self.msgsForFutureViews, lambda x: x <= self.viewNo)
        self.logNodeInfo()
        # Keep on doing catchup until >(n-f) nodes LedgerStatus same on have a
        # prepared certificate the first PRE-PREPARE of the new view
        logger.info('{}{} changed to view {}, will start catchup now'.
                    format(VIEW_CHANGE_PREFIX, self, self.viewNo))

        self._cancel(self._check_view_change_completed)
        self._schedule(action=self._check_view_change_completed,
                       seconds=self._view_change_timeout)

        # Set to 0 even when set to 0 in `on_view_change_complete` since
        # catchup might be started due to several reasons.
        self.catchup_rounds_without_txns = 0
        self._catch_up_start_ts = time.perf_counter()

    def on_view_change_complete(self):
        """
        View change completes for a replica when it has been decided which was
        the last ppSeqno and state and txn root for previous view
        """
        # TODO VCH update method description

        if not self.replicas.all_instances_have_primary:
            raise LogicError(
                "{} Not all replicas have primaries: {}"
                .format(self, self.replicas.primaries)
            )

        self._cancel(self._check_view_change_completed)

        self.master_replica.on_view_change_done()
        if self.view_changer.propagate_primary:  # TODO VCH
            for replica in self.replicas:
                replica.on_propagate_primary_done()
        self.view_changer.last_completed_view_no = self.view_changer.view_no
        # Remove already ordered requests from requests list after view change
        # If view change happen when one half of nodes ordered on master
        # instance and backup but other only on master then we need to clear
        # requests list.  We do this to stop transactions ordering  on backup
        # replicas that have already been ordered on master.
        # Test for this case in plenum/test/view_change/
        # test_no_propagate_request_on_different_last_ordered_before_vc.py
        if not self.view_changer.propagate_primary:
            for replica in self.replicas:
                replica.clear_requests_and_fix_last_ordered()

    def on_inconsistent_3pc_state_from_network(self):
        if self.config.ENABLE_INCONSISTENCY_WATCHER_NETWORK:
            self.on_inconsistent_3pc_state()

    def on_inconsistent_3pc_state(self):
        logger.warning("There is high probability that current 3PC state is inconsistent,"
                       "immediate restart is recommended")

    def create_replicas(self) -> Replicas:
        return Replicas(self, self.monitor, self.config)

    def utc_epoch(self) -> int:
        """
        Returns the UTC epoch according to it's local clock
        """
        return get_utc_epoch()

    def initPoolManager(self, ha, cliname, cliha):
        HasPoolManager.__init__(self, ha, cliname, cliha)

    def __repr__(self):
        return self.name

    def getDomainReqHandler(self):
        return DomainRequestHandler(self.domainLedger,
                                    self.states[DOMAIN_LEDGER_ID],
                                    self.config,
                                    self.reqProcessors,
                                    self.bls_bft.bls_store,
                                    self.getStateTsDbStorage())

    def getStateTsDbStorage(self):
        if self.stateTsDbStorage is None:
            self.stateTsDbStorage = StateTsDbStorage(
                self.name,
                initKeyValueStorageIntKeys(self.config.stateTsStorage,
                                           self.dataLocation,
                                           self.config.stateTsDbName,
                                           db_config=self.config.db_state_ts_db_config)
            )
        return self.stateTsDbStorage

    def loadSeqNoDB(self):
        return ReqIdrToTxn(
            initKeyValueStorage(
                self.config.reqIdToTxnStorage,
                self.dataLocation,
                self.config.seqNoDbName,
                db_config=self.config.db_seq_no_db_config)
        )

    # noinspection PyAttributeOutsideInit
    def setPoolParams(self):
        # TODO should be always called when nodeReg is changed - automate
        self.allNodeNames = set(self.nodeReg.keys())
        self.network_i3pc_watcher.set_nodes(self.allNodeNames)
        self.totalNodes = len(self.allNodeNames)
        self.f = getMaxFailures(self.totalNodes)
        self.requiredNumberOfInstances = self.f + 1  # per RBFT
        self.minimumNodes = (2 * self.f) + 1  # minimum for a functional pool
        self.quorums = Quorums(self.totalNodes)
        logger.info(
            "{} updated its pool parameters: f {}, totalNodes {}, "
            "allNodeNames {}, requiredNumberOfInstances {}, minimumNodes {}, "
            "quorums {}".format(
                self, self.f, self.totalNodes,
                self.allNodeNames, self.requiredNumberOfInstances,
                self.minimumNodes, self.quorums))

    @property
    def poolLedger(self):
        return self.poolManager.ledger \
            if isinstance(self.poolManager, TxnPoolManager) \
            else None

    @property
    def domainLedger(self):
        return self.primaryStorage

    def build_ledger_status(self, ledger_id):
        ledger = self.getLedger(ledger_id)
        ledger_size = ledger.size
        three_pc_key = self.three_phase_key_for_txn_seq_no(ledger_id,
                                                           ledger_size)
        v, p = three_pc_key if three_pc_key else (None, None)
        return LedgerStatus(ledger_id, ledger.size, v, p, ledger.root_hash, CURRENT_PROTOCOL_VERSION)

    @property
    def poolLedgerStatus(self):
        if self.poolLedger:
            return self.build_ledger_status(POOL_LEDGER_ID)

    @property
    def domainLedgerStatus(self):
        return self.build_ledger_status(DOMAIN_LEDGER_ID)

    def getLedgerRootHash(self, ledgerId, isCommitted=True):
        ledgerInfo = self.ledgerManager.getLedgerInfoByType(ledgerId)
        if not ledgerInfo:
            raise RuntimeError('Ledger with id {} does not exist')
        ledger = ledgerInfo.ledger
        if isCommitted:
            return ledger.root_hash
        return ledger.uncommittedRootHash or ledger.root_hash

    def stateRootHash(self, ledgerId, isCommitted=True):
        state = self.states.get(ledgerId)
        if not state:
            raise RuntimeError('State with id {} does not exist')
        return state.committedHeadHash if isCommitted else state.headHash

    @property
    def is_synced(self):
        return Mode.is_done_syncing(self.mode)

    @property
    def isParticipating(self) -> bool:
        return self.mode == Mode.participating

    def start_participating(self):
        logger.info('{} started participating'.format(self))
        self.mode = Mode.participating

    @property
    def nodeStackClass(self) -> NetworkInterface:
        return nodeStackClass

    @property
    def clientStackClass(self) -> NetworkInterface:
        return clientStackClass

    def getPrimaryStorage(self):
        """
        This is usually an implementation of Ledger
        """
        if self.config.primaryStorage is None:
            # TODO: add a place for initialization of all ledgers, so it's
            # clear what ledgers we have and how they are initialized
            genesis_txn_initiator = GenesisTxnInitiatorFromFile(
                self.genesis_dir, self.config.domainTransactionsFile)
            return Ledger(
                CompactMerkleTree(
                    hashStore=self.getHashStore('domain')),
                dataDir=self.dataLocation,
                fileName=self.config.domainTransactionsFile,
                ensureDurability=self.config.EnsureLedgerDurability,
                genesis_txn_initiator=genesis_txn_initiator)
        else:
            # TODO: we need to rethink this functionality
            return initStorage(self.config.primaryStorage,
                               name=self.name + NODE_PRIMARY_STORAGE_SUFFIX,
                               dataDir=self.dataLocation,
                               config=self.config)

    def _add_pool_ledger(self):
        if isinstance(self.poolManager, TxnPoolManager):
            self.ledgerManager.addLedger(
                POOL_LEDGER_ID,
                self.poolLedger,
                preCatchupStartClbk=self.prePoolLedgerCatchup,
                postCatchupCompleteClbk=self.postPoolLedgerCaughtUp,
                postTxnAddedToLedgerClbk=self.postTxnFromCatchupAddedToLedger)
            self.on_new_ledger_added(POOL_LEDGER_ID)

    def _add_domain_ledger(self):
        self.ledgerManager.addLedger(
            DOMAIN_LEDGER_ID,
            self.domainLedger,
            preCatchupStartClbk=self.preDomainLedgerCatchup,
            postCatchupCompleteClbk=self.postDomainLedgerCaughtUp,
            postTxnAddedToLedgerClbk=self.postTxnFromCatchupAddedToLedger)
        self.on_new_ledger_added(DOMAIN_LEDGER_ID)

    def getHashStore(self, name) -> HashStore:
        """
        Create and return a hashStore implementation based on configuration
        """
        return initHashStore(self.dataLocation, name, self.config)

    def get_new_ledger_manager(self) -> LedgerManager:
        ledger_sync_order = self.ledger_ids
        return LedgerManager(self, ownedByNode=True,
                             postAllLedgersCaughtUp=self.allLedgersCaughtUp,
                             preCatchupClbk=self.preLedgerCatchUp,
                             postCatchupClbk=self.postLedgerCatchUp,
                             ledger_sync_order=ledger_sync_order)

    def init_ledger_manager(self):
        self._add_pool_ledger()
        self._add_config_ledger()
        self._add_domain_ledger()

    def on_new_ledger_added(self, ledger_id):
        # If a ledger was added after a replicas were created
        self.replicas.register_new_ledger(ledger_id)

    def register_state(self, ledger_id, state):
        self.states[ledger_id] = state

    def register_req_handler(self, req_handler: RequestHandler,
                             ledger_id: int = None):
        if ledger_id is not None:
            self.ledger_to_req_handler[ledger_id] = req_handler
        for txn_type in req_handler.operation_types:
            self.register_txn_type(txn_type, req_handler, ledger_id)

    def register_txn_type(self, txn_type, req_handler: RequestHandler,
                          ledger_id: int = None):
        if txn_type in self.txn_type_to_req_handler:
            raise ValueError('{} already registered for {}'
                             .format(txn_type, self.txn_type_to_req_handler[txn_type]))
        self.txn_type_to_req_handler[txn_type] = req_handler
        if ledger_id is not None:
            self.txn_type_to_ledger_id[txn_type] = ledger_id

    def register_executer(self, ledger_id: int, executer: Callable):
        self.requestExecuter[ledger_id] = executer

    def get_req_handler(self, ledger_id=None, txn_type=None) -> Optional[RequestHandler]:
        if ledger_id is not None:
            return self.ledger_to_req_handler.get(ledger_id)
        if txn_type is not None:
            return self.txn_type_to_req_handler.get(txn_type)

    def get_executer(self, ledger_id):
        executer = self.requestExecuter.get(ledger_id)
        if executer:
            return executer
        else:
            return partial(self.default_executer, ledger_id)

    def loadDomainState(self):
        return PruningState(
            initKeyValueStorage(
                self.config.domainStateStorage,
                self.dataLocation,
                self.config.domainStateDbName,
                db_config=self.config.db_state_config)
        )

    def _create_bls_bft(self):
        bls_factory = create_default_bls_bft_factory(self)
        bls_bft = bls_factory.create_bls_bft()
        if bls_bft.can_sign_bls():
            logger.display("{}BLS Signatures will be used for Node {}".format(BLS_PREFIX, self.name))
        else:
            # TODO: for now we allow that BLS is optional, so that we don't require it
            logger.warning(
                '{}Transactions will not be BLS signed by this Node, since BLS keys were not found. '
                'Please make sure that a script to init BLS keys was called (init_bls_keys),'
                ' and NODE txn was sent with BLS public keys.'.format(BLS_PREFIX))
        return bls_bft

    def update_bls_key(self, new_bls_key):
        bls_keys_dir = os.path.join(self.keys_dir, self.name)
        bls_crypto_factory = create_default_bls_crypto_factory(bls_keys_dir)
        self.bls_bft.bls_crypto_signer = None

        try:
            bls_crypto_signer = bls_crypto_factory.create_bls_crypto_signer_from_saved_keys()
        except LoadBLSKeyError:
            logger.warning("{}Can not enable BLS signer on the Node. BLS keys are not initialized, "
                           "although NODE txn with blskey={} is sent. Please make sure that a script to init BLS keys (init_bls_keys) "
                           "was called ".format(BLS_PREFIX, new_bls_key))
            return

        if bls_crypto_signer.pk != new_bls_key:
            logger.warning("{}Can not enable BLS signer on the Node. BLS key initialized for the Node ({}), "
                           "differs from the one sent to the Ledger via NODE txn ({}). "
                           "Please make sure that a script to init BLS keys (init_bls_keys) is called, "
                           "and the same key is saved via NODE txn."
                           .format(BLS_PREFIX, bls_crypto_signer.pk, new_bls_key))
            return

        self.bls_bft.bls_crypto_signer = bls_crypto_signer
        logger.display("{}BLS key is rotated/set for Node {}. "
                       "BLS Signatures will be used for Node.".format(BLS_PREFIX, self.name))

    def ledger_id_for_request(self, request: Request):
        if request.operation.get(TXN_TYPE) is None:
            raise ValueError(
                "{} TXN_TYPE is not defined for request {}"
                .format(self, request)
            )

        typ = request.operation[TXN_TYPE]
        return self.txn_type_to_ledger_id[typ]

    def start(self, loop):
        # Avoid calling stop and then start on the same node object as start
        # does not re-initialise states
        oldstatus = self.status
        if oldstatus in Status.going():
            logger.debug("{} is already {}, so start has no effect".
                         format(self, self.status.name))
        else:
            super().start(loop)

            # Start the ledgers
            for ledger in self.ledgers:
                ledger.start(loop)

            self.nodestack.start()
            self.clientstack.start()

            self.view_changer = self.newViewChanger()
            self.elector = self.newPrimaryDecider()

            self.schedule_initial_propose_view_change()

            self.schedule_node_status_dump()
            self.dump_additional_info()

            # if first time running this node
            if not self.nodestack.remotes:
                logger.info("{} first time running..." "".format(self), extra={
                    "cli": "LOW_STATUS", "tags": ["node-key-sharing"]})
            else:
                self.nodestack.maintainConnections(force=True)

            self.start_catchup(just_started=True)

        self.logNodeInfo()

    def schedule_initial_propose_view_change(self):
        self.lost_primary_at = time.perf_counter()
        self._schedule(action=self.propose_view_change,
                       seconds=self.config.INITIAL_PROPOSE_VIEW_CHANGE_TIMEOUT)

    def schedule_node_status_dump(self):
        # one-shot dump right after start
        self._schedule(action=self._info_tool.dump_general_info,
                       seconds=self.config.DUMP_VALIDATOR_INFO_INIT_SEC)
        self.startRepeating(
            self._info_tool.dump_general_info,
            seconds=self.config.DUMP_VALIDATOR_INFO_PERIOD_SEC,
        )

    def dump_additional_info(self):
        self._info_tool.dump_additional_info()

    @property
    def rank(self) -> Optional[int]:
        return self.poolManager.rank

    def get_name_by_rank(self, rank, nodeReg=None):
        return self.poolManager.get_name_by_rank(rank, nodeReg=nodeReg)

    def get_rank_by_name(self, name, nodeReg=None):
        return self.poolManager.get_rank_by_name(name, nodeReg=nodeReg)

    def newViewChanger(self):
        if self.view_changer:
            return self.view_changer
        else:
            return ViewChanger(self)

    def newPrimaryDecider(self):
        if self.primaryDecider:
            return self.primaryDecider
        else:
            return PrimarySelector(self)

    @property
    def connectedNodeCount(self) -> int:
        """
        The plus one is for this node, for example, if this node has three
        connections, then there would be four total nodes
        :return: number of connected nodes this one
        """
        return len(self.nodestack.conns) + 1

    @property
    def ledgers(self):
        return [self.ledgerManager.ledgerRegistry[lid].ledger
                for lid in self.ledger_ids if lid in self.ledgerManager.ledgerRegistry]

    def onStopping(self):
        """
        Actions to be performed on stopping the node.

        - Close the UDP socket of the nodestack
        """
        # Log stats should happen before any kind of reset or clearing
        if self.config.STACK_COMPANION == 1:
            add_stop_time(self.ledger_dir, self.utc_epoch())

        self.logstats()

        self.reset()

        # Stop the ledgers
        for ledger in self.ledgers:
            try:
                ledger.stop()
            except Exception as ex:
                logger.exception('{} got exception while stopping ledger: {}'.format(self, ex))

        self.nodestack.stop()
        self.clientstack.stop()

        self.closeAllKVStores()

        self.mode = None
        self.ledgerManager.prepare_ledgers_for_sync()

    def closeAllKVStores(self):
        # Clear leveldb lock files
        logger.info("{} closing key-value storages".format(self), extra={"cli": False})
        for ledgerId in self.ledgerManager.ledgerRegistry:
            state = self.getState(ledgerId)
            if state:
                state.close()
        if self.seqNoDB:
            self.seqNoDB.close()
        if self.bls_bft.bls_store:
            self.bls_bft.bls_store.close()
        if self.stateTsDbStorage:
            self.stateTsDbStorage.close()

    def reset(self):
        logger.info("{} reseting...".format(self), extra={"cli": False})
        self.nodestack.nextCheck = 0
        logger.debug(
            "{} clearing aqStash of size {}".format(
                self, len(
                    self.aqStash)))
        self.nodestack.conns.clear()
        # TODO: Should `self.clientstack.conns` be cleared too
        # self.clientstack.conns.clear()
        self.aqStash.clear()
        self.actionQueue.clear()
        self.elector = None
        self.view_changer = None

    async def prod(self, limit: int=None) -> int:
        """.opened
        This function is executed by the node each time it gets its share of
        CPU time from the event loop.

        :param limit: the number of items to be serviced in this attempt
        :return: total number of messages serviced by this node
        """
        c = 0
        if self.status is not Status.stopped:
            c += await self.serviceReplicas(limit)
            c += await self.serviceNodeMsgs(limit)
            c += await self.serviceClientMsgs(limit)
            c += self._serviceActions()
            c += self.ledgerManager.service()
            c += self.monitor._serviceActions()
            c += await self.serviceViewChanger(limit)
            c += await self.service_observable(limit)
            c += await self.service_observer(limit)
            self.nodestack.flushOutBoxes()
        if self.isGoing():
            self.nodestack.serviceLifecycle()
            self.clientstack.serviceClientStack()
        return c

    async def serviceReplicas(self, limit) -> int:
        """
        Processes messages from replicas outbox and gives it time
        for processing inbox

        :param limit: the maximum number of messages to process
        :return: the sum of messages successfully processed
        """
        inbox_processed = self.replicas.service_inboxes(limit)
        outbox_processed = self.service_replicas_outbox(limit)
        return outbox_processed + inbox_processed

    async def serviceNodeMsgs(self, limit: int) -> int:
        """
        Process `limit` number of messages from the nodeInBox.

        :param limit: the maximum number of messages to process
        :return: the number of messages successfully processed
        """
        n = await self.nodestack.service(limit)
        await self.processNodeInBox()
        return n

    async def serviceClientMsgs(self, limit: int) -> int:
        """
        Process `limit` number of messages from the clientInBox.

        :param limit: the maximum number of messages to process
        :return: the number of messages successfully processed
        """
        # do not process any client requests if view change is in progress
        # TODO: process requests, but return a graceful Reject message, that can not process a message now because of
        # View Change
        if self.view_changer.view_change_in_progress:
            return 0
        c = await self.clientstack.service(limit)
        await self.processClientInBox()
        return c

    async def serviceViewChanger(self, limit) -> int:
        """
        Service the view_changer's inBox, outBox and action queues.

        :return: the number of messages successfully serviced
        """
        if not self.isReady():
            return 0
        o = self.serviceViewChangerOutBox(limit)
        i = await self.serviceViewChangerInbox(limit)
        # TODO: Why is protected method accessed here?
        a = self.view_changer._serviceActions()
        return o + i + a

    async def service_observable(self, limit) -> int:
        """
        Service the observable's inBox and outBox

        :return: the number of messages successfully serviced
        """
        if not self.isReady():
            return 0
        o = self._service_observable_out_box(limit)
        i = await self._observable.serviceQueues(limit)
        return o + i

    def _service_observable_out_box(self, limit: int=None) -> int:
        """
        Service at most `limit` number of messages from the observable's outBox.

        :return: the number of messages successfully serviced.
        """
        msg_count = 0
        while True:
            if limit and msg_count >= limit:
                break

            msg = self._observable.get_output()
            if not msg:
                break

            msg_count += 1
            msg, observer_ids = msg
            # TODO: it's assumed that all Observers are connected the same way as Validators
            self.sendToNodes(msg, observer_ids)
        return msg_count

    async def service_observer(self, limit) -> int:
        """
        Service the observer's inBox and outBox

        :return: the number of messages successfully serviced
        """
        if not self.isReady():
            return 0
        return await self._observer.serviceQueues(limit)

    def onConnsChanged(self, joined: Set[str], left: Set[str]):
        """
        A series of operations to perform once a connection count has changed.

        - Set f to max number of failures this system can handle.
        - Set status to one of started, started_hungry or starting depending on
            the number of protocol instances.
        - Check protocol instances. See `checkInstances()`

        """
        _prev_status = self.status
        if self.isGoing():
            if self.connectedNodeCount == self.totalNodes:
                self.status = Status.started
            elif self.connectedNodeCount >= self.minimumNodes:
                self.status = Status.started_hungry
            else:
                self.status = Status.starting
        self.elector.nodeCount = self.connectedNodeCount

        if self.master_primary_name in joined:
            self.lost_primary_at = None
        if self.master_primary_name in left:
            logger.display('{} lost connection to primary of master'.format(self))
            self.lost_master_primary()
        elif _prev_status == Status.starting and self.status == Status.started_hungry \
                and self.lost_primary_at is not None \
                and self.master_primary_name is not None:
            """
            Such situation may occur if the pool has come back to reachable consensus but
            primary is still disconnected, so view change proposal makes sense now.
            """
            self._schedule_view_change()

        if self.isReady():
            self.checkInstances()
        else:
            logger.info("{} joined nodes {} but status is {}".format(self, joined, self.status))
        # Send ledger status whether ready (connected to enough nodes) or not
        for node in joined:
            self.send_current_state_to_lagging_node(node)
            self.send_ledger_status_to_newly_connected_node(node)

        for node in left:
            self.network_i3pc_watcher.disconnect(node)

        for node in joined:
            self.network_i3pc_watcher.connect(node)

    def request_ledger_status_from_nodes(self, ledger_id, nodes=None):
        for node_name in nodes if nodes else self.nodeReg:
            if node_name == self.name:
                continue
            try:
                self._ask_for_ledger_status(node_name, ledger_id)
            except RemoteNotFound:
                logger.debug(
                    '{} did not find any remote for {} to send '
                    'request for ledger status'.format(
                        self, node_name))
                continue

    def _ask_for_ledger_status(self, node_name: str, ledger_id):
        """
        Ask other node for LedgerStatus
        """
        self.request_msg(LEDGER_STATUS, {f.LEDGER_ID.nm: ledger_id},
                         [node_name, ])
        logger.info("{} asking {} for ledger status of ledger {}".format(self, node_name, ledger_id))

    def send_ledger_status_to_newly_connected_node(self, node_name):
        self.sendLedgerStatus(node_name,
                              self.ledgerManager.ledger_sync_order[0])

    def nodeJoined(self, txn_data):
        logger.display("{} new node joined by txn {}".format(self, txn_data))
        self.setPoolParams()
        new_replicas = self.adjustReplicas()
        ledgerInfo = self.ledgerManager.getLedgerInfoByType(POOL_LEDGER_ID)
        if new_replicas > 0 and not self.view_changer.view_change_in_progress and \
                ledgerInfo.state == LedgerState.synced:
            # Select primaries must be only after pool ledger catchup
            # or if poolLedger already caughtup and we are ordering node transaction
            self.select_primaries()

    def nodeLeft(self, txn_data):
        logger.display("{} node left by txn {}".format(self, txn_data))
        self.setPoolParams()
        self.adjustReplicas()

    @property
    def clientStackName(self):
        return self.getClientStackNameOfNode(self.name)

    @staticmethod
    def getClientStackNameOfNode(nodeName: str):
        return nodeName + CLIENT_STACK_SUFFIX

    def getClientStackHaOfNode(self, nodeName: str) -> HA:
        return self.cliNodeReg.get(self.getClientStackNameOfNode(nodeName))

    def send_current_state_to_lagging_node(self, nodeName: str):
        rid = self.nodestack.getRemote(nodeName).uid
        vch_messages = self.view_changer.get_msgs_for_lagged_nodes()
        message = CurrentState(viewNo=self.viewNo, primary=vch_messages)

        logger.info("{} sending current state {} to lagged node {}".format(self, message, nodeName))
        self.send(message, rid)

    def process_current_state_message(self, msg: CurrentState, frm):
        logger.info("{} processing current state {} from {}".format(self, msg, frm))
        try:
            # TODO: parsing of internal messages should be done with other way
            # We should consider reimplementing validation so that it can
            # work with internal messages. It should not only validate them,
            # but also set parsed as field values
            messages = [ViewChangeDone(**message) for message in msg.primary]
            for message in messages:
                # TODO DRY, view change done messages are managed by routes
                self.sendToViewChanger(message, frm, from_current_state=True)
        except TypeError:
            self.discard(msg,
                         reason="{}invalid election messages".format(
                             PRIMARY_SELECTION_PREFIX),
                         logMethod=logger.warning)

    def _statusChanged(self, old: Status, new: Status) -> None:
        """
        Perform some actions based on whether this node is ready or not.

        :param old: the previous status
        :param new: the current status
        """

    def checkInstances(self) -> None:
        # TODO: Is this method really needed?
        """
        Check if this node has the minimum required number of protocol
        instances, i.e. f+1. If not, add a replica. If no election is in
        progress, this node will try to nominate one of its replicas as primary.
        This method is called whenever a connection with a  new node is
        established.
        """
        logger.debug("{} choosing to start election on the basis of count {} and nodes {}".
                     format(self, self.connectedNodeCount, self.nodestack.conns))

    def adjustReplicas(self):
        """
        Add or remove replicas depending on `f`
        """
        # TODO: refactor this
        newReplicas = 0
        while len(self.replicas) < self.requiredNumberOfInstances:
            self.replicas.grow()
            newReplicas += 1
            self.processStashedMsgsForReplica(len(self.replicas) - 1)
        while len(self.replicas) > self.requiredNumberOfInstances:
            self.replicas.shrink()
            newReplicas -= 1
        pop_keys(self.msgsForFutureReplicas, lambda x: x < len(self.replicas))
        return newReplicas

    def _dispatch_stashed_msg(self, msg, frm):
        # TODO DRY, in normal (non-stashed) case it's managed
        # implicitly by routes
        if isinstance(msg, (InstanceChange, ViewChangeDone)):
            self.sendToViewChanger(msg, frm)
            return True
        elif isinstance(msg, ThreePhaseType):
            self.sendToReplica(msg, frm)
            return True
        else:
            return False

    def processStashedMsgsForReplica(self, instId: int):
        if instId not in self.msgsForFutureReplicas:
            return
        i = 0
        while self.msgsForFutureReplicas[instId]:
            msg, frm = self.msgsForFutureReplicas[instId].popleft()
            if not self._dispatch_stashed_msg(msg, frm):
                self.discard(msg, reason="Unknown message type for replica id "
                                         "{}".format(instId),
                             logMethod=logger.warning)
            i += 1
        logger.info("{} processed {} stashed msgs for replica {}".format(self, i, instId))

    def processStashedMsgsForView(self, view_no: int):
        if view_no not in self.msgsForFutureViews:
            return
        i = 0
        while self.msgsForFutureViews[view_no]:
            msg, frm = self.msgsForFutureViews[view_no].popleft()
            if not self._dispatch_stashed_msg(msg, frm):
                self.discard(msg,
                             reason="{}Unknown message type for view no {}"
                             .format(VIEW_CHANGE_PREFIX, view_no),
                             logMethod=logger.warning)
            i += 1
        logger.info("{} processed {} stashed msgs for view no {}".format(self, i, view_no))

    def _check_view_change_completed(self):
        """
        This thing checks whether new primary was elected.
        If it was not - starts view change again
        """
        logger.info('{} running the scheduled check for view change completion'.format(self))
        if not self.view_changer.view_change_in_progress:
            logger.info('{} already completion view change'.format(self))
            return False

        self.view_changer.on_view_change_not_completed_in_time()
        return True

    def service_replicas_outbox(self, limit: int=None) -> int:
        """
        Process `limit` number of replica messages
        """
        # TODO: rewrite this using Router

        num_processed = 0
        for message in self.replicas.get_output(limit):
            num_processed += 1
            if isinstance(message, (PrePrepare, Prepare, Commit, Checkpoint)):
                self.send(message)
            elif isinstance(message, Ordered):
                self.try_processing_ordered(message)
            elif isinstance(message, tuple) and isinstance(message[1], Reject):
                digest, reject = message
                result_reject = Reject(
                    reject.identifier,
                    reject.reqId,
                    self.reasonForClientFromException(
                        reject.reason))
                # TODO: What the case when reqKey will be not in requestSender dict
                if digest in self.requestSender:
                    self.transmitToClient(result_reject, self.requestSender[digest])
                    self.doneProcessingReq(digest)
            elif isinstance(message, Exception):
                self.processEscalatedException(message)
            else:
                # TODO: should not this raise exception?
                logger.error("Received msg {} and don't "
                             "know how to handle it".format(message))
        return num_processed

    def serviceViewChangerOutBox(self, limit: int=None) -> int:
        """
        Service at most `limit` number of messages from the view_changer's outBox.

        :return: the number of messages successfully serviced.
        """
        msgCount = 0
        while self.view_changer.outBox and (not limit or msgCount < limit):
            msgCount += 1
            msg = self.view_changer.outBox.popleft()
            if isinstance(msg, (InstanceChange, ViewChangeDone)):
                self.send(msg)
            else:
                logger.error("Received msg {} and don't know how to handle it".
                             format(msg))
        return msgCount

    async def serviceViewChangerInbox(self, limit: int=None) -> int:
        """
        Service at most `limit` number of messages from the view_changer's outBox.

        :return: the number of messages successfully serviced.
        """
        msgCount = 0
        while self.msgsToViewChanger and (not limit or msgCount < limit):
            msgCount += 1
            msg = self.msgsToViewChanger.popleft()
            self.view_changer.inBox.append(msg)
        await self.view_changer.serviceQueues(limit)
        return msgCount

    @property
    def hasPrimary(self) -> bool:
        """
        Whether this node has primary of any protocol instance
        """
        # TODO: remove this property?
        return self.replicas.some_replica_is_primary

    @property
    def has_master_primary(self) -> bool:
        """
        Whether this node has primary of master protocol instance
        """
        # TODO: remove this property?
        return self.replicas.master_replica_is_primary

    @property
    def master_primary_name(self) -> Optional[str]:
        """
        Return the name of the primary node of the master instance
        """

        master_primary_name = self.master_replica.primaryName
        if master_primary_name:
            return self.master_replica.getNodeName(master_primary_name)
        return None

    @property
    def master_last_ordered_3PC(self) -> Tuple[int, int]:
        return self.master_replica.last_ordered_3pc

    @property
    def master_replica(self):
        # TODO: this must be refactored.
        # Accessing Replica directly should be prohibited
        return self.replicas._master_replica

    def msgHasAcceptableInstId(self, msg, frm) -> bool:
        """
        Return true if the instance id of message corresponds to a correct
        replica.

        :param msg: the node message to validate
        :return:
        """
        # TODO: refactor this! this should not do anything except checking!
        instId = getattr(msg, f.INST_ID.nm, None)
        if not (isinstance(instId, int) and instId >= 0):
            return False
        if instId >= self.replicas.num_replicas:
            if instId not in self.msgsForFutureReplicas:
                self.msgsForFutureReplicas[instId] = deque()
            self.msgsForFutureReplicas[instId].append((msg, frm))
            logger.debug("{} queueing message {} for future protocol instance {}".format(self, msg, instId))
            return False
        return True

    def _should_accept_current_state(self):
        return (self.viewNo == 0) and (self.master_primary_name is None)

    def msgHasAcceptableViewNo(self, msg, frm, from_current_state: bool = False) -> bool:
        """
        Return true if the view no of message corresponds to the current view
        no or a view no in the future
        :param msg: the node message to validate
        :return:
        """
        # TODO: refactor this! this should not do anything except checking!
        view_no = getattr(msg, f.VIEW_NO.nm, None)
        if not (isinstance(view_no, int) and view_no >= 0):
            return False
        if self.viewNo - view_no > 1:
            self.discard(msg, "un-acceptable viewNo {}"
                         .format(view_no), logMethod=logger.warning)
        elif (view_no > self.viewNo) or (from_current_state and self._should_accept_current_state()):
            if view_no not in self.msgsForFutureViews:
                self.msgsForFutureViews[view_no] = deque()
            logger.debug('{} stashing a message for a future view: {}'.format(self, msg))
            self.msgsForFutureViews[view_no].append((msg, frm))
            if isinstance(msg, ViewChangeDone):
                future_vcd_msg = FutureViewChangeDone(vcd_msg=msg, from_current_state=from_current_state)
                self.msgsToViewChanger.append((future_vcd_msg, frm))
        else:
            return True
        return False

    def sendToReplica(self, msg, frm):
        """
        Send the message to the intended replica.

        :param msg: the message to send
        :param frm: the name of the node which sent this `msg`
        """
        # TODO: discard or stash messages here instead of doing
        # this in msgHas* methods!!!
        if self.msgHasAcceptableInstId(msg, frm):
            if self.msgHasAcceptableViewNo(msg, frm):
                self.replicas.pass_message((msg, frm), msg.instId)

    def sendToViewChanger(self, msg, frm, from_current_state: bool = False):
        """
        Send the message to the intended view changer.

        :param msg: the message to send
        :param frm: the name of the node which sent this `msg`
        """
        if (isinstance(msg, InstanceChange) or
                self.msgHasAcceptableViewNo(msg, frm, from_current_state=from_current_state)):
            logger.debug("{} sending message to view changer: {}".
                         format(self, (msg, frm)))
            self.msgsToViewChanger.append((msg, frm))

    def send_to_observer(self, msg, frm):
        """
        Send the message to the observer.

        :param msg: the message to send
        :param frm: the name of the node which sent this `msg`
        """
        logger.debug("{} sending message to observer: {}".
                     format(self, (msg, frm)))
        self._observer.append_input(msg, frm)

    def handleOneNodeMsg(self, wrappedMsg):
        """
        Validate and process one message from a node.

        :param wrappedMsg: Tuple of message and the name of the node that sent
        the message
        """
        try:
            vmsg = self.validateNodeMsg(wrappedMsg)
            if vmsg:
                logger.trace("{} msg validated {}".format(self, wrappedMsg),
                             extra={"tags": ["node-msg-validation"]})
                self.unpackNodeMsg(*vmsg)
            else:
                logger.debug("{} invalidated msg {}".format(self, wrappedMsg),
                             extra={"tags": ["node-msg-validation"]})
        except SuspiciousNode as ex:
            self.reportSuspiciousNodeEx(ex)
        except Exception as ex:
            msg, frm = wrappedMsg
            self.discard(msg, ex, logger.info)

    def validateNodeMsg(self, wrappedMsg):
        """
        Validate another node's message sent to this node.

        :param wrappedMsg: Tuple of message and the name of the node that sent
        the message
        :return: Tuple of message from node and name of the node
        """
        msg, frm = wrappedMsg
        if self.isNodeBlacklisted(frm):
            self.discard(msg[:256], "received from blacklisted node {}".format(frm), logger.display)
            return None

        try:
            message = node_message_factory.get_instance(**msg)
        except (MissingNodeOp, InvalidNodeOp) as ex:
            raise ex
        except Exception as ex:
            raise InvalidNodeMsg(str(ex))

        try:
            self.verifySignature(message)
        except BaseExc as ex:
            raise SuspiciousNode(frm, ex, message) from ex
        logger.debug("{} received node message from {}: {}".format(self, frm, message), extra={"cli": False})
        return message, frm

    def unpackNodeMsg(self, msg, frm) -> None:
        """
        If the message is a batch message validate each message in the batch,
        otherwise add the message to the node's inbox.

        :param msg: a node message
        :param frm: the name of the node that sent this `msg`
        """
        # TODO: why do we unpack batches here? Batching is a feature of
        # a transport, it should be encapsulated.

        if isinstance(msg, Batch):
            logger.trace("{} processing a batch {}".format(self, msg))
            for m in msg.messages:
                m = self.nodestack.deserializeMsg(m)
                self.handleOneNodeMsg((m, frm))
        else:
            self.postToNodeInBox(msg, frm)

    def postToNodeInBox(self, msg, frm):
        """
        Append the message to the node inbox

        :param msg: a node message
        :param frm: the name of the node that sent this `msg`
        """
        logger.trace("{} appending to nodeInbox {}".format(self, msg))
        self.nodeInBox.append((msg, frm))

    async def processNodeInBox(self):
        """
        Process the messages in the node inbox asynchronously.
        """
        while self.nodeInBox:
            m = self.nodeInBox.popleft()
            try:
                await self.nodeMsgRouter.handle(m)
            except SuspiciousNode as ex:
                self.reportSuspiciousNodeEx(ex)
                self.discard(m, ex, logger.debug)

    def handleOneClientMsg(self, wrappedMsg):
        """
        Validate and process a client message

        :param wrappedMsg: a message from a client
        """
        try:
            vmsg = self.validateClientMsg(wrappedMsg)
            if vmsg:
                self.unpackClientMsg(*vmsg)
        except BlowUp:
            raise
        except Exception as ex:
            msg, frm = wrappedMsg
            friendly = friendlyEx(ex)
            if isinstance(ex, SuspiciousClient):
                self.reportSuspiciousClient(frm, friendly)

            self.handleInvalidClientMsg(ex, wrappedMsg)

    def handleInvalidClientMsg(self, ex, wrappedMsg):
        msg, frm = wrappedMsg
        exc = ex.__cause__ if ex.__cause__ else ex
        friendly = friendlyEx(ex)
        reason = self.reasonForClientFromException(ex)
        if isinstance(msg, Request):
            msg = msg.as_dict
        identifier = idr_from_req_data(msg)
        # we send reqId == 1 when we need to reply on invalid LEDGER_STATUS
        reqId = msg.get(f.REQ_ID.nm) or 1
        if not reqId:
            reqId = getattr(exc, f.REQ_ID.nm, None)
            if not reqId:
                reqId = getattr(ex, f.REQ_ID.nm, None)
        self.send_nack_to_client((identifier, reqId), reason, frm)
        self.discard(wrappedMsg, friendly, logger.info, cliOutput=True)
        self._specific_invalid_client_msg_handling(ex, msg, frm)

    def _specific_invalid_client_msg_handling(self, ex, msg, frm):
        op = msg.get('op')
        if (op == LEDGER_STATUS):
            self._invalid_client_ledger_status_handling(ex, msg, frm)

    def _invalid_client_ledger_status_handling(self, ex, msg, frm):
        # This specific validation handles incorrect client LEDGER_STATUS message
        logger.info("{} received bad LEDGER_STATUS message from client {}. "
                    "Reason: {}. ".format(self, frm, ex.args[0]))
        # Since client can't yet handle denial of LEDGER_STATUS,
        # node send his LEDGER_STATUS back
        self.send_ledger_status_to_client(msg.get(f.LEDGER_ID.nm),
                                          msg.get(f.TXN_SEQ_NO.nm),
                                          msg.get(f.VIEW_NO.nm),
                                          msg.get(f.PP_SEQ_NO.nm),
                                          msg.get(f.MERKLE_ROOT.nm),
                                          CURRENT_PROTOCOL_VERSION,
                                          frm)

    def validateClientMsg(self, wrappedMsg):
        """
        Validate a message sent by a client.
        :param wrappedMsg: a message from a client
        :return: Tuple of clientMessage and client address
        """
        msg, frm = wrappedMsg
        if self.isClientBlacklisted(frm):
            self.discard(msg[:256], "received from blacklisted client {}".format(frm), logger.display)
            return None

        needStaticValidation = False
        if all([msg.get(OPERATION), msg.get(f.REQ_ID.nm),
                idr_from_req_data(msg)]):
            cls = self.client_request_class
            needStaticValidation = True
        elif OP_FIELD_NAME in msg:
            op = msg[OP_FIELD_NAME]
            cls = node_message_factory.get_type(op)
            if cls not in (Batch, LedgerStatus, CatchupReq):
                raise InvalidClientMsgType(cls, msg.get(f.REQ_ID.nm))
        else:
            raise InvalidClientRequest(msg.get(f.IDENTIFIER.nm),
                                       msg.get(f.REQ_ID.nm))
        try:
            cMsg = cls(**msg)
        except TypeError as ex:
            raise InvalidClientRequest(msg.get(f.IDENTIFIER.nm),
                                       msg.get(f.REQ_ID.nm),
                                       str(ex))
        except Exception as ex:
            raise InvalidClientRequest(msg.get(f.IDENTIFIER.nm),
                                       msg.get(f.REQ_ID.nm)) from ex

        if needStaticValidation:
            self.doStaticValidation(cMsg)

        self.execute_hook(NodeHooks.PRE_SIG_VERIFICATION, cMsg)
        self.verifySignature(cMsg)
        self.execute_hook(NodeHooks.POST_SIG_VERIFICATION, cMsg)
        # Suspicions should only be raised when lot of sig failures are
        # observed
        # try:
        #     self.verifySignature(cMsg)
        # except UnknownIdentifier as ex:
        #     raise
        # except Exception as ex:
        #     raise SuspiciousClient from ex
        logger.trace("{} received CLIENT message: {}".
                     format(self.clientstack.name, cMsg))
        return cMsg, frm

    def unpackClientMsg(self, msg, frm):
        """
        If the message is a batch message validate each message in the batch,
        otherwise add the message to the node's clientInBox.

        :param msg: a client message
        :param frm: the name of the client that sent this `msg`
        """
        if isinstance(msg, Batch):
            for m in msg.messages:
                # This check is done since Client uses NodeStack (which can
                # send and receive BATCH) to talk to nodes but Node uses
                # ClientStack (which cannot send or receive BATCH).
                # TODO: The solution is to have both kind of stacks be able to
                # parse BATCH messages
                if m in (ZStack.pingMessage, ZStack.pongMessage):
                    continue
                m = self.clientstack.deserializeMsg(m)
                self.handleOneClientMsg((m, frm))
        else:
            self.postToClientInBox(msg, frm)

    def postToClientInBox(self, msg, frm):
        """
        Append the message to the node's clientInBox

        :param msg: a client message
        :param frm: the name of the node that sent this `msg`
        """
        self.clientInBox.append((msg, frm))

    async def processClientInBox(self):
        """
        Process the messages in the node's clientInBox asynchronously.
        All messages in the inBox have already been validated, including
        signature check.
        """
        while self.clientInBox:
            m = self.clientInBox.popleft()
            req, frm = m
            logger.debug("{} processing {} request {}".
                         format(self.clientstack.name, frm, req),
                         extra={"cli": True,
                                "tags": ["node-msg-processing"]})

            try:
                await self.clientMsgRouter.handle(m)
            except InvalidClientMessageException as ex:
                self.handleInvalidClientMsg(ex, m)

    # TODO: change sending format from Reject to (digest, Reject)
    # if you will use this method
    def _reject_msg(self, msg, frm, reason):
        reqKey = (msg.identifier, msg.reqId)
        reject = Reject(*reqKey,
                        reason)
        self.transmitToClient(reject, frm)

    def postPoolLedgerCaughtUp(self, **kwargs):
        self.mode = Mode.discovered
        # The node might have discovered more nodes, so see if schedule
        # election if needed.
        if isinstance(self.poolManager, TxnPoolManager):
            self.checkInstances()

        # TODO: why we do it this way?
        # Initialising node id in case where node's information was not present
        # in pool ledger at the time of starting, happens when a non-genesis
        # node starts
        self.id

    def postDomainLedgerCaughtUp(self, **kwargs):
        """
        Process any stashed ordered requests and set the mode to
        `participating`
        :return:
        """
        pass

    def preLedgerCatchUp(self, ledger_id):
        # Process any Ordered requests. This causes less transactions to be
        # requested during catchup. Also commits any uncommitted state that
        # can be committed
        logger.info('{} going to process any ordered requests before starting catchup.'.format(self))
        self.force_process_ordered()
        self.processStashedOrderedReqs()

        # revert uncommitted txns and state for unordered requests
        r = self.master_replica.revert_unordered_batches()
        logger.info('{} reverted {} batches before starting catch up for ledger {}'.format(self, r, ledger_id))

    def postLedgerCatchUp(self, ledger_id, last_caughtup_3pc):
        # update 3PC key interval tree to return last ordered to other nodes in Ledger Status
        self._update_txn_seq_range_to_3phase_after_catchup(ledger_id, last_caughtup_3pc)

    def postTxnFromCatchupAddedToLedger(self, ledger_id: int, txn: Any):
        rh = self.postRecvTxnFromCatchup(ledger_id, txn)
        if rh:
            rh.updateState([txn], isCommitted=True)
            state = self.getState(ledger_id)
            state.commit(rootHash=state.headHash)
            if ledger_id == DOMAIN_LEDGER_ID and rh.ts_store:
                rh.ts_store.set(get_txn_time(txn),
                                state.headHash)
        self.updateSeqNoMap([txn], ledger_id)
        self._clear_req_key_for_txn(ledger_id, txn)

    def _clear_req_key_for_txn(self, ledger_id, txn):
        req_key = get_digest(txn)
        if req_key is not None:
            self.master_replica.discard_req_key(ledger_id, req_key)

    def postRecvTxnFromCatchup(self, ledgerId: int, txn: Any):
        if ledgerId == POOL_LEDGER_ID:
            self.poolManager.onPoolMembershipChange(txn)
        if ledgerId == DOMAIN_LEDGER_ID:
            self.post_txn_from_catchup_added_to_domain_ledger(txn)
        rh = self.get_req_handler(ledgerId)
        return rh

    # TODO: should be renamed to `post_all_ledgers_caughtup`
    def allLedgersCaughtUp(self):
        if self.num_txns_caught_up_in_last_catchup() == 0:
            self.catchup_rounds_without_txns += 1
        last_caught_up_3PC = self.ledgerManager.last_caught_up_3PC
        if compare_3PC_keys(self.master_last_ordered_3PC,
                            last_caught_up_3PC) > 0:
            for replica in self.replicas:
                if replica.isMaster:
                    replica.caught_up_till_3pc(last_caught_up_3PC)
                else:
                    replica.catchup_clear_for_backup()
            logger.info('{}{} caught up till {}'
                        .format(CATCH_UP_PREFIX, self, last_caught_up_3PC),
                        extra={'cli': True})

        # TODO: Maybe a slight optimisation is to check result of
        # `self.num_txns_caught_up_in_last_catchup()`
        self.processStashedOrderedReqs()

        # More than one catchup may be needed during the current ViewChange protocol
        # TODO: separate view change and catchup logic
        if self.is_catchup_needed():
            logger.info('{} needs to catchup again'.format(self))
            self.start_catchup()
        else:
            logger.info('{}{} does not need any more catchups'
                        .format(CATCH_UP_PREFIX, self),
                        extra={'cli': True})
            self.no_more_catchups_needed()
            # select primaries after pool ledger caughtup
            if not self.view_change_in_progress:
                self.select_primaries()

    def _update_txn_seq_range_to_3phase_after_catchup(self, ledger_id, last_caughtup_3pc):
        logger.info(
            "{} is updating txn to batch seqNo map after catchup to {} for ledger_id {} "
            .format(self.name, str(last_caughtup_3pc), str(ledger_id)))
        if not last_caughtup_3pc:
            return
        # do not set if this is a 'fake' one, see replica.on_view_change_start
        # also (0,0) will be passed from ledger_manager._buildConsistencyProof if 3PC is None
        if last_caughtup_3pc[1] == 0:
            return

        ledger_size = self.getLedger(ledger_id).size
        three_pc_key = self.three_phase_key_for_txn_seq_no(ledger_id,
                                                           ledger_size)
        if three_pc_key:
            return

        self._update_txn_seq_range_to_3phase(first_txn_seq_no=ledger_size,
                                             last_txn_seq_no=ledger_size,
                                             ledger_id=ledger_id,
                                             view_no=last_caughtup_3pc[0],
                                             pp_seq_no=last_caughtup_3pc[1])

    def is_catchup_needed(self) -> bool:
        """
        Check if received a quorum of view change done messages and if yes
        check if caught up till the
        Check if all requests ordered till last prepared certificate
        Check if last catchup resulted in no txns
        """
        # More than one catchup may be needed during the current ViewChange protocol
        # No more catchup is needed if this is a common catchup (not part of View Change)
        if not self.view_change_in_progress:
            return False

        if self.caught_up_for_current_view():
            logger.info('{} is caught up for the current view {}'.format(self, self.viewNo))
            return False
        logger.info('{} is not caught up for the current view {}'.format(self, self.viewNo))

        if self.num_txns_caught_up_in_last_catchup() == 0:
            if self.has_ordered_till_last_prepared_certificate():
                logger.info('{} ordered till last prepared certificate'.format(self))
                return False

        if self.is_catch_up_limit():
            return False

        return True

    def caught_up_for_current_view(self) -> bool:
        if not self.view_changer._hasViewChangeQuorum:
            logger.info('{} does not have view change quorum for view {}'.format(self, self.viewNo))
            return False
        vc = self.view_changer.get_sufficient_same_view_change_done_messages()
        if not vc:
            logger.info('{} does not have acceptable ViewChangeDone for view {}'.format(self, self.viewNo))
            return False
        ledger_info = vc[1]
        for lid, size, root_hash in ledger_info:
            ledger = self.ledgerManager.ledgerRegistry[lid].ledger
            if size == 0:
                continue
            if ledger.size < size:
                return False
            if ledger.hashToStr(
                    ledger.tree.merkle_tree_hash(0, size)) != root_hash:
                return False
        return True

    def has_ordered_till_last_prepared_certificate(self) -> bool:
        lst = self.master_replica.last_prepared_before_view_change
        if lst is None:
            return True
        return compare_3PC_keys(lst, self.master_replica.last_ordered_3pc) >= 0

    def is_catch_up_limit(self):
        ts_since_catch_up_start = time.perf_counter() - self._catch_up_start_ts
        if ts_since_catch_up_start >= self.config.MIN_TIMEOUT_CATCHUPS_DONE_DURING_VIEW_CHANGE:
            logger.info('{} has completed {} catchup rounds for {} seconds'.
                        format(self, self.catchup_rounds_without_txns, ts_since_catch_up_start))
            # No more 3PC messages will be processed since maximum catchup
            # rounds have been done
            self.master_replica.last_prepared_before_view_change = None
            return True
        return False

    def num_txns_caught_up_in_last_catchup(self) -> int:
        count = sum([l.num_txns_caught_up for l in
                     self.ledgerManager.ledgerRegistry.values()])
        logger.info('{} caught up to {} txns in the last catchup'.format(self, count))
        return count

    def no_more_catchups_needed(self):
        # This method is called when no more catchups needed
        self._catch_up_start_ts = 0
        self.mode = Mode.synced
        self.view_changer.on_catchup_complete()
        # TODO: need to think of a better way
        # If the node was not participating but has now found a primary,
        # then set mode to participating, can happen if a catchup is triggered
        # without a view change or node start
        if not self.isParticipating and self.master_replica.hasPrimary:
            logger.info('{} starting to participate since catchup is done, '
                        'primaries are selected but mode was not set to '
                        'participating'.format(self))
            self.start_participating()

    def getLedger(self, ledgerId) -> Ledger:
        return self.ledgerManager.getLedgerInfoByType(ledgerId).ledger

    def getState(self, ledgerId) -> PruningState:
        return self.states.get(ledgerId)

    def post_txn_from_catchup_added_to_domain_ledger(self, txn):
        if get_type(txn) == NYM:
            self.addNewRole(txn)

    def getLedgerStatus(self, ledgerId: int):
        if ledgerId == POOL_LEDGER_ID and not self.poolLedger:
            # Since old style nodes don't know have pool ledger
            return None
        return self.build_ledger_status(ledgerId)

    def sendLedgerStatus(self, nodeName: str, ledgerId: int):
        ledgerStatus = self.getLedgerStatus(ledgerId)
        if ledgerStatus:
            self.sendToNodes(ledgerStatus, [nodeName])
        else:
            logger.info("{} not sending ledger {} status to {} as it is null".format(self, ledgerId, nodeName))

    def send_ledger_status_to_client(self, lid, txn_s_n, v, p, merkle, protocol, client):
        ls = LedgerStatus(lid, txn_s_n, v, p, merkle, protocol)
        self.transmitToClient(ls, client)

    def doStaticValidation(self, request: Request):
        identifier, req_id, operation = request.identifier, request.reqId, request.operation
        if TXN_TYPE not in operation:
            raise InvalidClientRequest(identifier, req_id)

        self.execute_hook(NodeHooks.PRE_STATIC_VALIDATION, request=request)
        if operation[TXN_TYPE] != GET_TXN:
            # GET_TXN is generic, needs no request handler

            req_handler = self.get_req_handler(txn_type=operation[TXN_TYPE])
            if not req_handler:
                # TODO: This code should probably be removed.
                if self.opVerifiers:
                    try:
                        for v in self.opVerifiers:
                            v.verify(operation)
                    except Exception as ex:
                        raise InvalidClientRequest(identifier, req_id) from ex
                else:
                    raise InvalidClientRequest(identifier, req_id, 'invalid {}: {}'.
                                               format(TXN_TYPE, operation[TXN_TYPE]))
            else:
                req_handler.doStaticValidation(request)

        self.execute_hook(NodeHooks.POST_STATIC_VALIDATION, request=request)

    def doDynamicValidation(self, request: Request):
        """
        State based validation
        """
        self.execute_hook(NodeHooks.PRE_DYNAMIC_VALIDATION, request=request)
        operation = request.operation
        req_handler = self.get_req_handler(txn_type=operation[TXN_TYPE])
        req_handler.validate(request)
        self.execute_hook(NodeHooks.POST_DYNAMIC_VALIDATION, request=request)

    def applyReq(self, request: Request, cons_time: int):
        """
        Apply request to appropriate ledger and state. `cons_time` is the
        UTC epoch at which consensus was reached.
        """
        self.execute_hook(NodeHooks.PRE_REQUEST_APPLICATION, request=request,
                          cons_time=cons_time)
        req_handler = self.get_req_handler(txn_type=request.operation[TXN_TYPE])
        seq_no, txn = req_handler.apply(request, cons_time)
        ledger_id = self.ledger_id_for_request(request)
        self.execute_hook(NodeHooks.POST_REQUEST_APPLICATION, request=request,
                          cons_time=cons_time, ledger_id=ledger_id,
                          seq_no=seq_no, txn=txn)

    def apply_stashed_reqs(self, request_ids, cons_time: int, ledger_id):
        requests = []
        for req_key in request_ids:
            req = self.requests[req_key].finalised
            _, seq_no = self.seqNoDB.get(req.digest)
            if seq_no is None:
                requests.append(req)
        self.apply_reqs(requests, cons_time, ledger_id)

    def apply_reqs(self, requests, cons_time: int, ledger_id):
        for req in requests:
            self.applyReq(req, cons_time)
        state_root = self.stateRootHash(ledger_id, isCommitted=False)
        self.onBatchCreated(ledger_id, state_root)

    def handle_request_if_forced(self, request: Request):
        if request.isForced():
            req_handler = self.get_req_handler(
                txn_type=request.operation[TXN_TYPE])
            req_handler.validate(request)
            req_handler.applyForced(request)

    def processRequest(self, request: Request, frm: str):
        """
        Handle a REQUEST from the client.
        If the request has already been executed, the node re-sends the reply to
        the client. Otherwise, the node acknowledges the client request, adds it
        to its list of client requests, and sends a PROPAGATE to the
        remaining nodes.

        :param request: the REQUEST from the client
        :param frm: the name of the client that sent this REQUEST
        """
        logger.debug("{} received client request: {} from {}".
                     format(self.name, request, frm))
        self.nodeRequestSpikeMonitorData['accum'] += 1

        # TODO: What if client sends requests with same request id quickly so
        # before reply for one is generated, the other comes. In that
        # case we need to keep track of what requests ids node has seen
        # in-memory and once request with a particular request id is processed,
        # it should be removed from that in-memory DS.

        # If request is already processed(there is a reply for the
        # request in
        # the node's transaction store then return the reply from the
        # transaction store)
        # TODO: What if the reply was a REQNACK? Its not gonna be found in the
        # replies.

        txn_type = request.operation[TXN_TYPE]

        if self.is_action(txn_type):
            self.process_action(request, frm)

        elif txn_type == GET_TXN:
            self.handle_get_txn_req(request, frm)
            self.total_read_request_number += 1

        elif self.is_query(txn_type):
            self.process_query(request, frm)
            self.total_read_request_number += 1

        elif self.can_write_txn(txn_type):
            reply = self.getReplyFromLedgerForRequest(request)
            if reply:
                logger.debug("{} returning REPLY from already processed "
                             "REQUEST: {}".format(self, request))
                self.transmitToClient(reply, frm)
                return

            # If the node is not already processing the request
            if not self.isProcessingReq(request.key):
                self.startedProcessingReq(request.key, frm)
                # forced request should be processed before consensus
                self.handle_request_if_forced(request)

            # If not already got the propagate request(PROPAGATE) for the
            # corresponding client request(REQUEST)
            self.recordAndPropagate(request, frm)
            self.send_ack_to_client((request.identifier, request.reqId), frm)

        else:
            raise InvalidClientRequest(
                request.identifier,
                request.reqId,
                'Pool is in readonly mode, try again in 60 seconds')

    def is_query(self, txn_type) -> bool:
        # Does the transaction type correspond to a read?
        handler = self.get_req_handler(txn_type=txn_type)
        return handler and isinstance(handler,
                                      LedgerRequestHandler
                                      ) and handler.is_query(txn_type)

    def is_action(self, txn_type) -> bool:
        return txn_type in self.actionReqHandler.operation_types

    def can_write_txn(self, txn_type):
        return True

    def process_query(self, request: Request, frm: str):
        # Process a read request from client
        handler = self.get_req_handler(txn_type=request.operation[TXN_TYPE])
        try:
            handler.doStaticValidation(request)
            self.send_ack_to_client((request.identifier, request.reqId), frm)
        except Exception as ex:
            self.send_nack_to_client((request.identifier, request.reqId),
                                     str(ex), frm)
        result = handler.get_query_response(request)
        self.transmitToClient(Reply(result), frm)

    def process_action(self, request, frm):
        # Process an execute action request
        self.send_ack_to_client((request.identifier, request.reqId), frm)
        try:
            self.actionReqHandler.validate(request)
            result = self.actionReqHandler.apply(request)
            self.transmitToClient(Reply(result), frm)
        except Exception as ex:
            self.transmitToClient(Reject(request.identifier,
                                         request.reqId,
                                         str(ex)), frm)

    # noinspection PyUnusedLocal
    def processPropagate(self, msg: Propagate, frm):
        """
        Process one propagateRequest sent to this node asynchronously

        - If this propagateRequest hasn't been seen by this node, then broadcast
        it to all nodes after verifying the the signature.
        - Add the client to blacklist if its signature is invalid

        :param msg: the propagateRequest
        :param frm: the name of the node which sent this `msg`
        """
        logger.debug("{} received propagated request: {}".
                     format(self.name, msg))

        reqDict = msg.request
        request = self.client_request_class(**reqDict)

        clientName = msg.senderClient

        if not self.isProcessingReq(request.key):
            ledger_id, seq_no = self.seqNoDB.get(request.key)
            if ledger_id is not None and seq_no is not None:
                logger.debug("{} ignoring propagated request {} "
                             "since it has been already ordered"
                             .format(self.name, msg))
                return

            self.startedProcessingReq(request.key, clientName)
            # forced request should be processed before consensus
            self.handle_request_if_forced(request)

        else:
            if clientName is not None and \
                    not self.is_sender_known_for_req(request.key):
                # Since some propagates might not include the client name
                self.set_sender_for_req(request.key,
                                        clientName)

        self.requests.add_propagate(request, frm)

        self.propagate(request, clientName)
        self.tryForwarding(request)

    def startedProcessingReq(self, key, frm):
        self.requestSender[key] = frm

    def isProcessingReq(self, key) -> bool:
        return key in self.requestSender

    def doneProcessingReq(self, key):
        self.requestSender.pop(key)

    def is_sender_known_for_req(self, key):
        return self.requestSender.get(key) is not None

    def set_sender_for_req(self, key, frm):
        self.requestSender[key] = frm

    def send_ack_to_client(self, req_key, to_client):
        self.transmitToClient(RequestAck(*req_key), to_client)

    def send_nack_to_client(self, req_key, reason, to_client):
        self.transmitToClient(RequestNack(*req_key, reason), to_client)

    def handle_get_txn_req(self, request: Request, frm: str):
        """
        Handle GET_TXN request
        """
        ledger_id = request.operation.get(f.LEDGER_ID.nm, DOMAIN_LEDGER_ID)
        if ledger_id not in self.ledger_to_req_handler:
            self.send_nack_to_client((request.identifier, request.reqId),
                                     'Invalid ledger id {}'.format(ledger_id),
                                     frm)
            return

        seq_no = request.operation.get(DATA)
        self.send_ack_to_client((request.identifier, request.reqId), frm)
        ledger = self.getLedger(ledger_id)

        try:
            txn = self.getReplyFromLedger(ledger, seq_no)
        except KeyError:
            txn = None

        if txn is None:
            logger.debug(
                "{} can not handle GET_TXN request: ledger doesn't "
                "have txn with seqNo={}". format(self, str(seq_no)))

        result = {
            f.IDENTIFIER.nm: request.identifier,
            f.REQ_ID.nm: request.reqId,
            TXN_TYPE: request.operation[TXN_TYPE],
            DATA: None
        }

        if txn:
            result[DATA] = txn.result
            result[f.SEQ_NO.nm] = get_seq_no(txn.result)

        self.transmitToClient(Reply(result), frm)

    def processOrdered(self, ordered: Ordered):
        """
        Execute ordered request

        :param ordered: an ordered request
        :return: whether executed
        """

        if ordered.instId >= self.instances.count:
            logger.warning('{} got ordered request for instance {} which '
                           'does not exist'.format(self, ordered.instId))
            return False

        if ordered.instId != self.instances.masterId:
            # Requests from backup replicas are not executed
            logger.trace("{} got ordered requests from backup replica {}"
                         .format(self, ordered.instId))
            self.monitor.requestOrdered(ordered.reqIdr,
                                        ordered.instId,
                                        self.requests,
                                        byMaster=False)
            return False

        logger.trace("{} got ordered requests from master replica"
                     .format(self))
        requests = [self.requests[request_id].finalised
                    for request_id in ordered.reqIdr
                    if request_id in self.requests and
                    self.requests[request_id].finalised]

        if len(requests) != len(ordered.reqIdr):
            logger.warning('{} did not find {} finalized '
                           'requests, but still ordered'
                           .format(self, len(ordered.reqIdr) - len(requests)))
            return False

        logger.debug("{} executing Ordered batch {} {} of {} requests"
                     .format(self.name,
                             ordered.viewNo,
                             ordered.ppSeqNo,
                             len(ordered.reqIdr)))

        self.executeBatch(ordered.viewNo,
                          ordered.ppSeqNo,
                          ordered.ppTime,
                          requests,
                          ordered.ledgerId,
                          ordered.stateRootHash,
                          ordered.txnRootHash)

        self.monitor.requestOrdered(ordered.reqIdr,
                                    ordered.instId,
                                    self.requests,
                                    byMaster=True)

        return True

    def force_process_ordered(self):
        """
        Take any messages from replica that have been ordered and process
        them, this should be done rarely, like before catchup starts
        so a more current LedgerStatus can be sent.
        can be called either
        1. when node is participating, this happens just before catchup starts
        so the node can have the latest ledger status or
        2. when node is not participating but a round of catchup is about to be
        started, here is forces all the replica ordered messages to be appended
        to the stashed ordered requests and the stashed ordered requests are
        processed with appropriate checks
        """

        for instance_id, messages in self.replicas.take_ordereds_out_of_turn():
            num_processed = 0
            for message in messages:
                self.try_processing_ordered(message)
                num_processed += 1
            logger.debug('{} processed {} Ordered batches for instance {} '
                         'before starting catch up'
                         .format(self, num_processed, instance_id))

    def try_processing_ordered(self, msg):
        if self.isParticipating:
            self.processOrdered(msg)
        else:
            logger.debug("{} stashing {} since mode is {}".format(self, msg, self.mode))
            self.stashedOrderedReqs.append(msg)

    def processEscalatedException(self, ex):
        """
        Process an exception escalated from a Replica
        """
        if isinstance(ex, SuspiciousNode):
            self.reportSuspiciousNodeEx(ex)
        else:
            raise RuntimeError("unhandled replica-escalated exception") from ex

    def _update_new_ordered_reqs_count(self):
        """
        Checks if any requests have been ordered since last performance check
        and updates the performance check data store if needed.
        :return: True if new ordered requests, False otherwise
        """
        last_num_ordered = self._last_performance_check_data.get('num_ordered')
        num_ordered = sum(num for num, _ in self.monitor.numOrderedRequests)
        if num_ordered != last_num_ordered:
            self._last_performance_check_data['num_ordered'] = num_ordered
            return True
        else:
            return False

    def checkPerformance(self) -> Optional[bool]:
        """
        Check if master instance is slow and send an instance change request.
        :returns True if master performance is OK, False if performance
        degraded, None if the check was needed
        """
        logger.trace("{} checking its performance".format(self))

        # Move ahead only if the node has synchronized its state with other
        # nodes
        if not self.isParticipating:
            return

        if not self._update_new_ordered_reqs_count():
            logger.trace("{} ordered no new requests".format(self))
            return

        if self.instances.masterId is not None:
            self.sendNodeRequestSpike()
            if self.monitor.isMasterDegraded():
                logger.display('{} master instance performance degraded'.format(self))
                self.view_changer.on_master_degradation()
                return False
            else:
                logger.trace("{}'s master has higher performance than backups".
                             format(self))
        return True

    def checkNodeRequestSpike(self):
        logger.trace("{} checking its request amount".format(self))

        if not self.isParticipating:
            return

        if self.instances.masterId is not None:
            self.sendNodeRequestSpike()

    def sendNodeRequestSpike(self):
        requests = self.nodeRequestSpikeMonitorData['accum']
        self.nodeRequestSpikeMonitorData['accum'] = 0
        return pluginManager.sendMessageUponSuspiciousSpike(
            notifierPluginTriggerEvents['nodeRequestSpike'],
            self.nodeRequestSpikeMonitorData,
            requests,
            self.config.notifierEventTriggeringConfig['nodeRequestSpike'],
            self.name,
            self.config.SpikeEventsEnabled
        )

    def primary_selected(self, instance_id):
        # If the node has primary replica of master instance
        if instance_id == 0:
            # TODO: 0 should be replaced with configurable constant
            self.monitor.hasMasterPrimary = self.has_master_primary
        if not self.lost_primary_at:
            return
        if self.nodestack.isConnectedTo(self.master_primary_name) or \
                self.master_primary_name == self.name:
            self.lost_primary_at = None

    def propose_view_change(self):
        # Sends instance change message when primary has been
        # disconnected for long enough
        self._cancel(self.propose_view_change)
        if not self.lost_primary_at:
            logger.info('{} The primary is already connected '
                        'so view change will not be proposed'.format(self))
            return

        logger.display("{} primary has been disconnected for too long".format(self))

        if not self.isReady():
            logger.info('{} The node is not ready yet '
                        'so view change will not be proposed now, but re-scheduled.'.format(self))
            # self._schedule_view_change()
            return

        self.view_changer.on_primary_loss()

    def _schedule_view_change(self):
        logger.info('{} scheduling a view change in {} sec'.format(self, self.config.ToleratePrimaryDisconnection))
        self._schedule(self.propose_view_change,
                       self.config.ToleratePrimaryDisconnection)

    # TODO: consider moving this to pool manager
    def lost_master_primary(self):
        """
        Schedule an primary connection check which in turn can send a view
        change message
        """
        self.lost_primary_at = time.perf_counter()
        self._schedule_view_change()

    def select_primaries(self, nodeReg: Dict[str, HA]=None):
        primaries = set()
        primary_rank = None
        '''
        Build a set of names of primaries, it is needed to avoid
        duplicates of primary nodes for different replicas.
        '''
        for instance_id, replica in enumerate(self.replicas):
            if replica.primaryName is not None:
                name = replica.primaryName.split(":", 1)[0]
                primaries.add(name)
                '''
                Remember the rank of primary of master instance, it is needed
                for calculation of primaries for backup instances.
                '''
                if instance_id == 0:
                    primary_rank = self.get_rank_by_name(name, nodeReg)

        for instance_id, replica in enumerate(self.replicas):
            if replica.primaryName is not None:
                logger.debug('{} already has a primary'.format(replica))
                continue
            if instance_id == 0:
                new_primary_name, new_primary_instance_name =\
                    self.elector.next_primary_replica_name_for_master(nodeReg=nodeReg)
                primary_rank = self.get_rank_by_name(
                    new_primary_name, nodeReg)
                # TODO add more tests or refactor
                # to return name and rank at once and remove assert
                assert primary_rank is not None
            else:
                new_primary_name, new_primary_instance_name =\
                    self.elector.next_primary_replica_name_for_backup(
                        instance_id, primary_rank, primaries, nodeReg=nodeReg)
            primaries.add(new_primary_name)
            logger.display("{}{} selected primary {} for instance {} (view {})"
                           .format(PRIMARY_SELECTION_PREFIX, replica,
                                   new_primary_instance_name, instance_id, self.viewNo),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})
            if instance_id == 0:
                # The node needs to be set in participating mode since when
                # the replica is made aware of the primary, it will start
                # processing stashed requests and hence the node needs to be
                # participating.
                self.start_participating()

            replica.primaryChanged(new_primary_instance_name)
            self.primary_selected(instance_id)

            logger.display("{}{} declares view change {} as completed for "
                           "instance {}, "
                           "new primary is {}, "
                           "ledger info is {}"
                           .format(VIEW_CHANGE_PREFIX,
                                   replica,
                                   self.viewNo,
                                   instance_id,
                                   new_primary_instance_name,
                                   self.ledger_summary),
                           extra={"cli": "ANNOUNCE",
                                  "tags": ["node-election"]})

    def start_catchup(self, just_started=False):
        # Process any already Ordered requests by the replica

        if self.mode == Mode.starting:
            logger.info('{} does not start the catchup procedure '
                        'because it is already in this state'.format(self))
            return
        self.force_process_ordered()

        # # revert uncommitted txns and state for unordered requests
        r = self.master_replica.revert_unordered_batches()
        logger.info('{} reverted {} batches before starting catch up'.format(self, r))

        self.mode = Mode.starting
        self.ledgerManager.prepare_ledgers_for_sync()
        self.ledgerManager.catchup_ledger(self.ledgerManager.ledger_sync_order[0],
                                          request_ledger_statuses=not just_started)

    def ordered_prev_view_msgs(self, inst_id, pp_seqno):
        logger.debug('{} ordered previous view batch {} by instance {}'.
                     format(self, pp_seqno, inst_id))

    def verifySignature(self, msg):
        """
        Validate the signature of the request
        Note: Batch is whitelisted because the inner messages are checked

        :param msg: a message requiring signature verification
        :return: None; raises an exception if the signature is not valid
        """
        if isinstance(msg, self.authnWhitelist):
            return
        if isinstance(msg, Propagate):
            typ = 'propagate'
            req = msg.request
        else:
            typ = ''
            req = msg

        if not isinstance(req, Mapping):
            req = msg.as_dict

        identifiers = self.authNr(req).authenticate(req)
        logger.debug("{} authenticated {} signature on {} request {}".
                     format(self, identifiers, typ, req['reqId']),
                     extra={"cli": True,
                            "tags": ["node-msg-processing"]})

    def authNr(self, req):
        return self.clientAuthNr

    def three_phase_key_for_txn_seq_no(self, ledger_id, seq_no):
        if ledger_id in self.txn_seq_range_to_3phase_key:
            # point query in interval tree
            s = self.txn_seq_range_to_3phase_key[ledger_id][seq_no]
            if s:
                # There should not be more than one interval for any seq no in
                # the tree
                assert len(s) == 1  # TODO add test for that and remove assert
                return s.pop().data
        return None

    def executeBatch(self, view_no, pp_seq_no: int, pp_time: float,
                     reqs: List[Request], ledger_id, state_root,
                     txn_root) -> None:
        """
        Execute the REQUEST sent to this Node

        :param view_no: the view number (See glossary)
        :param pp_time: the time at which PRE-PREPARE was sent
        :param reqs: list of client REQUESTs
        """
        for req in reqs:
            self.execute_hook(NodeHooks.PRE_REQUEST_COMMIT, request=req,
                              pp_time=pp_time, state_root=state_root,
                              txn_root=txn_root)

        self.execute_hook(NodeHooks.PRE_BATCH_COMMITTED, ledger_id=ledger_id,
                          pp_time=pp_time, reqs=reqs, state_root=state_root,
                          txn_root=txn_root)

        try:
            committedTxns = self.get_executer(ledger_id)(pp_time, reqs,
                                                         state_root, txn_root)
        except Exception as exc:
            logger.error(
                "{} commit failed for batch request, error {}, view no {}, "
                "ppSeqNo {}, ledger {}, state root {}, txn root {}, "
                "requests: {}".format(
                    self, repr(exc), view_no, pp_seq_no, ledger_id, state_root,
                    txn_root, [req.digest for req in reqs]
                )
            )
            raise

        if not committedTxns:
            return

        # TODO is it possible to get len(committedTxns) != len(reqs)
        # someday
        for request in reqs:
            self.requests.mark_as_executed(request)
        logger.debug("{} committed batch request, view no {}, ppSeqNo {}, "
                     "ledger {}, state root {}, txn root {}, requests: {}".
                     format(self, view_no, pp_seq_no, ledger_id, state_root,
                            txn_root, [req.digest for req in reqs]))

        for txn in committedTxns:
            self.execute_hook(NodeHooks.POST_REQUEST_COMMIT, txn=txn,
                              pp_time=pp_time, state_root=state_root,
                              txn_root=txn_root)

        first_txn_seq_no = get_seq_no(committedTxns[0])
        last_txn_seq_no = get_seq_no(committedTxns[-1])

        self._update_txn_seq_range_to_3phase(first_txn_seq_no, last_txn_seq_no,
                                             ledger_id,
                                             view_no, pp_seq_no)

        batch_committed_msg = BatchCommitted([req.as_dict for req in reqs],
                                             ledger_id,
                                             pp_time,
                                             state_root,
                                             txn_root,
                                             first_txn_seq_no,
                                             last_txn_seq_no)
        self._observable.append_input(batch_committed_msg, self.name)

    def _update_txn_seq_range_to_3phase(self, first_txn_seq_no, last_txn_seq_no,
                                        ledger_id, view_no, pp_seq_no):
        if ledger_id not in self.txn_seq_range_to_3phase_key:
            self.txn_seq_range_to_3phase_key[ledger_id] = IntervalTree()
        # adding one to end of range since its exclusive
        intrv_tree = self.txn_seq_range_to_3phase_key[ledger_id]
        intrv_tree[first_txn_seq_no:last_txn_seq_no + 1] = (view_no, pp_seq_no)
        logger.debug('{} storing 3PC key {} for ledger {} range {}'.
                     format(self, (view_no, pp_seq_no), ledger_id,
                            (first_txn_seq_no, last_txn_seq_no)))
        if len(intrv_tree) > self.config.ProcessedBatchMapsToKeep:
            # Remove the first element from the interval tree
            old = intrv_tree[intrv_tree.begin()].pop()
            intrv_tree.remove(old)
            logger.debug('{} popped {} from txn to batch seqNo map for ledger_id {}'.
                         format(self, old, str(ledger_id)))

    def updateSeqNoMap(self, committedTxns, ledger_id):
        if all([get_req_id(txn) for txn in committedTxns]):
            self.seqNoDB.addBatch((get_digest(txn), ledger_id, get_seq_no(txn))
                                  for txn in committedTxns)

    def commitAndSendReplies(self, ledger_id, ppTime, reqs: List[Request],
                             stateRoot, txnRoot) -> List:
        logger.trace('{} going to commit and send replies to client'.format(self))
        reqHandler = self.get_req_handler(ledger_id)
        committedTxns = reqHandler.commit(len(reqs), stateRoot, txnRoot, ppTime)
        self.execute_hook(NodeHooks.POST_BATCH_COMMITTED, ledger_id=ledger_id,
                          pp_time=ppTime, committed_txns=committedTxns,
                          state_root=stateRoot, txn_root=txnRoot)
        self.updateSeqNoMap(committedTxns, ledger_id)
        updated_committed_txns = list(map(self.update_txn_with_extra_data, committedTxns))
        self.execute_hook(NodeHooks.PRE_SEND_REPLY, committed_txns=updated_committed_txns,
                          pp_time=ppTime)
        self.sendRepliesToClients(updated_committed_txns, ppTime)
        self.execute_hook(NodeHooks.POST_SEND_REPLY,
                          committed_txns=updated_committed_txns,
                          pp_time=ppTime)
        return committedTxns

    def default_executer(self, ledger_id, pp_time, reqs: List[Request],
                         state_root, txn_root):
        return self.commitAndSendReplies(ledger_id,
                                         pp_time, reqs, state_root, txn_root)

    def executeDomainTxns(self, ppTime, reqs: List[Request], stateRoot,
                          txnRoot) -> List:
        committed_txns = self.default_executer(DOMAIN_LEDGER_ID, ppTime, reqs,
                                               stateRoot, txnRoot)

        # Refactor: This is only needed for plenum as some old style tests
        # require authentication based on an in-memory map. This would be
        # removed later when we migrate old-style tests
        for txn in committed_txns:
            if get_type(txn) == NYM:
                self.addNewRole(txn)

        return committed_txns

    def onBatchCreated(self, ledger_id, state_root):
        """
        A batch of requests has been created and has been applied but
        committed to ledger and state.
        :param ledger_id:
        :param state_root: state root after the batch creation
        :return:
        """
        if ledger_id == POOL_LEDGER_ID:
            if isinstance(self.poolManager, TxnPoolManager):
                self.get_req_handler(POOL_LEDGER_ID).onBatchCreated(state_root)
        elif self.get_req_handler(ledger_id):
            self.get_req_handler(ledger_id).onBatchCreated(state_root)
        else:
            logger.debug('{} did not know how to handle for ledger {}'.format(self, ledger_id))
        self.execute_hook(NodeHooks.POST_BATCH_CREATED, ledger_id, state_root)

    def onBatchRejected(self, ledger_id):
        """
        A batch of requests has been rejected, if stateRoot is None, reject
        the current batch.
        :param ledger_id:
        :param stateRoot: state root after the batch was created
        :return:
        """
        if ledger_id == POOL_LEDGER_ID:
            if isinstance(self.poolManager, TxnPoolManager):
                self.get_req_handler(POOL_LEDGER_ID).onBatchRejected()
        elif self.get_req_handler(ledger_id):
            self.get_req_handler(ledger_id).onBatchRejected()
        else:
            logger.debug('{} did not know how to handle for ledger {}'.format(self, ledger_id))
        self.execute_hook(NodeHooks.POST_BATCH_REJECTED, ledger_id)

    def sendRepliesToClients(self, committedTxns, ppTime):
        for txn in committedTxns:
            self.sendReplyToClient(Reply(txn),
                                   get_digest(txn))

    def sendReplyToClient(self, reply, reqKey):
        if self.isProcessingReq(reqKey):
            sender = self.requestSender[reqKey]
            if sender:
                logger.trace(
                    '{} sending reply for {} to client'.format(
                        self, reqKey))
                self.transmitToClient(reply, sender)
            else:
                logger.info('{} not sending reply for {}, since do not '
                            'know client'.format(self, reqKey))
            self.doneProcessingReq(reqKey)

    def addNewRole(self, txn):
        """
        Adds a new client or steward to this node based on transaction type.
        """
        # If the client authenticator is a simple authenticator then add verkey.
        #  For a custom authenticator, handle appropriately.
        # NOTE: The following code should not be used in production
        if isinstance(self.clientAuthNr.core_authenticator, SimpleAuthNr):
            txn_data = get_payload_data(txn)
            identifier = txn_data[TARGET_NYM]
            verkey = txn_data.get(VERKEY)
            v = DidVerifier(verkey, identifier=identifier)
            if identifier not in self.clientAuthNr.core_authenticator.clients:
                role = txn_data.get(ROLE)
                if role not in (STEWARD, TRUSTEE, None):
                    logger.debug("Role if present must be {} and not {}".
                                 format(Roles.STEWARD.name, role))
                    return
                self.clientAuthNr.core_authenticator.addIdr(identifier,
                                                            verkey=v.verkey,
                                                            role=role)

    def initStateFromLedger(self, state: State, ledger: Ledger, reqHandler):
        """
        If the trie is empty then initialize it by applying
        txns from ledger.
        """
        if state.isEmpty:
            logger.info('{} found state to be empty, recreating from '
                        'ledger'.format(self))
            for seq_no, txn in ledger.getAllTxn():
                txn = self.update_txn_with_extra_data(txn)
                reqHandler.updateState([txn, ], isCommitted=True)
                state.commit(rootHash=state.headHash)

    def initDomainState(self):
        self.initStateFromLedger(self.states[DOMAIN_LEDGER_ID],
                                 self.domainLedger, self.get_req_handler(DOMAIN_LEDGER_ID))

    def addGenesisNyms(self):
        # THIS SHOULD NOT BE DONE FOR PRODUCTION
        for _, txn in self.domainLedger.getAllTxn():
            if get_type(txn) == NYM:
                self.addNewRole(txn)

    def init_core_authenticator(self):
        state = self.getState(DOMAIN_LEDGER_ID)
        return CoreAuthNr(state=state)

    def defaultAuthNr(self) -> ReqAuthenticator:
        req_authnr = ReqAuthenticator()
        req_authnr.register_authenticator(self.init_core_authenticator())
        return req_authnr

    def processStashedOrderedReqs(self):
        i = 0
        while self.stashedOrderedReqs:
            msg = self.stashedOrderedReqs.popleft()
            if msg.instId == 0:
                if compare_3PC_keys(
                        (msg.viewNo,
                         msg.ppSeqNo),
                        self.ledgerManager.last_caught_up_3PC) >= 0:
                    logger.debug(
                        '{} ignoring stashed ordered msg {} since ledger '
                        'manager has last_caught_up_3PC as {}'.format(
                            self, msg, self.ledgerManager.last_caught_up_3PC))
                    continue
                logger.debug(
                    '{} applying stashed Ordered msg {}'.format(self, msg))
                # Since the PRE-PREPAREs ans PREPAREs corresponding to these
                # stashed ordered requests was not processed.
                self.apply_stashed_reqs(msg.reqIdr,
                                        msg.ppTime,
                                        msg.ledgerId)

            self.processOrdered(msg)
            i += 1

        logger.debug(
            "{} processed {} stashed ordered requests".format(
                self, i))
        # Resetting monitor after executing all stashed requests so no view
        # change can be proposed
        self.monitor.reset()
        return i

    def ensureKeysAreSetup(self):
        """
        Check whether the keys are setup in the local STP keep.
        Raises KeysNotFoundException if not found.
        """
        if not areKeysSetup(self.name, self.keys_dir):
            raise REx(REx.reason.format(self.name) + self.keygenScript)

    @staticmethod
    def reasonForClientFromException(ex: Exception):
        friendly = friendlyEx(ex)
        reason = "client request invalid: {}".format(friendly)
        return reason

    def reportSuspiciousNodeEx(self, ex: SuspiciousNode):
        """
        Report suspicion on a node on the basis of an exception
        """
        self.reportSuspiciousNode(ex.node, ex.reason, ex.code, ex.offendingMsg)

    def reportSuspiciousNode(self,
                             nodeName: str,
                             reason=None,
                             code: int = None,
                             offendingMsg=None):
        """
        Report suspicion on a node and add it to this node's blacklist.

        :param nodeName: name of the node to report suspicion on
        :param reason: the reason for suspicion
        """
        logger.warning("{} raised suspicion on node {} for {}; suspicion code "
                       "is {}".format(self, nodeName, reason, code))
        # TODO need a more general solution here

        # TODO: Should not blacklist client on a single InvalidSignature.
        # Should track if a lot of requests with incorrect signatures have been
        # made in a short amount of time, only then blacklist client.
        # if code == InvalidSignature.code:
        #     self.blacklistNode(nodeName,
        #                        reason=InvalidSignature.reason,
        #                        code=InvalidSignature.code)

        # TODO: Consider blacklisting nodes again.
        # if code in self.suspicions:
        #     self.blacklistNode(nodeName,
        #                        reason=self.suspicions[code],
        #                        code=code)

        if code in (s.code for s in (Suspicions.PPR_DIGEST_WRONG,
                                     Suspicions.PPR_REJECT_WRONG,
                                     Suspicions.PPR_TXN_WRONG,
                                     Suspicions.PPR_STATE_WRONG,
                                     Suspicions.PPR_PLUGIN_EXCEPTION)):
            logger.display('{}{} got one of primary suspicions codes {}'.format(VIEW_CHANGE_PREFIX, self, code))
            self.view_changer.on_suspicious_primary(Suspicions.get_by_code(code))

        if offendingMsg:
            self.discard(offendingMsg, reason, logger.debug)

    def reportSuspiciousClient(self, clientName: str, reason):
        """
        Report suspicion on a client and add it to this node's blacklist.

        :param clientName: name of the client to report suspicion on
        :param reason: the reason for suspicion
        """
        logger.warning("{} raised suspicion on client {} for {}"
                       .format(self, clientName, reason))
        self.blacklistClient(clientName)

    def isClientBlacklisted(self, clientName: str):
        """
        Check whether the given client is in this node's blacklist.

        :param clientName: the client to check for blacklisting
        :return: whether the client was blacklisted
        """
        return self.clientBlacklister.isBlacklisted(clientName)

    def blacklistClient(self, clientName: str,
                        reason: str = None, code: int = None):
        """
        Add the client specified by `clientName` to this node's blacklist
        """
        msg = "{} blacklisting client {}".format(self, clientName)
        if reason:
            msg += " for reason {}".format(reason)
        logger.display(msg)
        self.clientBlacklister.blacklist(clientName)

    def isNodeBlacklisted(self, nodeName: str) -> bool:
        """
        Check whether the given node is in this node's blacklist.

        :param nodeName: the node to check for blacklisting
        :return: whether the node was blacklisted
        """
        return self.nodeBlacklister.isBlacklisted(nodeName)

    def blacklistNode(self, nodeName: str, reason: str = None, code: int = None):
        """
        Add the node specified by `nodeName` to this node's blacklist
        """
        msg = "{} blacklisting node {}".format(self, nodeName)
        if reason:
            msg += " for reason {}".format(reason)
        if code:
            msg += " for code {}".format(code)
        logger.display(msg)
        self.nodeBlacklister.blacklist(nodeName)

    @property
    def blacklistedNodes(self):
        return {nm for nm in self.nodeReg.keys() if
                self.nodeBlacklister.isBlacklisted(nm)}

    def transmitToClient(self, msg: Any, remoteName: str):
        self.clientstack.transmitToClient(msg, remoteName)

    def send(self,
             msg: Any,
             *rids: Iterable[int],
             signer: Signer = None,
             message_splitter=None):

        if rids:
            remoteNames = [self.nodestack.remotes[rid].name for rid in rids]
            recipientsNum = len(remoteNames)
        else:
            # so it is broadcast
            remoteNames = [remote.name for remote in
                           self.nodestack.remotes.values()]
            recipientsNum = 'all'

        logger.debug("{} sending message {} to {} recipients: {}".
                     format(self, msg, recipientsNum, remoteNames))
        self.nodestack.send(msg, *rids, signer=signer, message_splitter=message_splitter)

    def sendToNodes(self, msg: Any, names: Iterable[str] = None, message_splitter=None):
        # TODO: This method exists in `Client` too, refactor to avoid
        # duplication
        rids = [rid for rid, r in self.nodestack.remotes.items(
        ) if r.name in names] if names else []
        self.send(msg, *rids, message_splitter=message_splitter)

    def getReplyFromLedgerForRequest(self, request):
        ledger_id, seq_no = self.seqNoDB.get(request.digest)
        if ledger_id is not None and seq_no is not None:
            ledger = self.getLedger(ledger_id)
            return self.getReplyFromLedger(ledger, seq_no)
        else:
            return None

    def getReplyFromLedger(self, ledger, seq_no):
        # DoS attack vector, client requesting already processed request id
        # results in iterating over ledger (or its subset)
        txn = ledger.getBySeqNo(int(seq_no))
        if txn:
            txn.update(ledger.merkleInfo(seq_no))
            txn = self.update_txn_with_extra_data(txn)
            return Reply(txn)
        else:
            return None

    def update_txn_with_extra_data(self, txn):
        """
        All the data of the transaction might not be stored in ledger so the
        extra data that is omitted from ledger needs to be fetched from the
        appropriate data store
        :param txn:
        :return:
        """
        # All the data of any transaction is stored in the ledger
        return txn

    def transform_txn_for_ledger(self, txn):
        txn_type = get_type(txn)
        return self.get_req_handler(txn_type=txn_type).\
            transform_txn_for_ledger(txn)

    def __enter__(self):
        return self

    # noinspection PyUnusedLocal
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def logstats(self):
        """
        Print the node's current statistics to log.
        """
        lines = [
            "node {} current stats".format(self),
            "--------------------------------------------------------",
            "node inbox size         : {}".format(len(self.nodeInBox)),
            "client inbox size       : {}".format(len(self.clientInBox)),
            "age (seconds)           : {}".format(time.time() - self.created),
            "next check for reconnect: {}".format(time.perf_counter() -
                                                  self.nodestack.nextCheck),
            "node connections        : {}".format(self.nodestack.conns),
            "f                       : {}".format(self.f),
            "master instance         : {}".format(self.instances.masterId),
            "replicas                : {}".format(len(self.replicas)),
            "view no                 : {}".format(self.viewNo),
            "rank                    : {}".format(self.rank),
            "msgs to replicas        : {}".format(self.replicas.sum_inbox_len),
            "msgs to view changer    : {}".format(len(self.msgsToViewChanger)),
            "action queue            : {} {}".format(len(self.actionQueue),
                                                     id(self.actionQueue)),
            "action queue stash      : {} {}".format(len(self.aqStash),
                                                     id(self.aqStash)),
        ]

        logger.info("\n".join(lines), extra={"cli": False})

    def collectNodeInfo(self):
        nodeAddress = None
        if self.poolLedger:
            for _, txn in self.poolLedger.getAllTxn():
                data = get_payload_data(txn)[DATA]
                if data[ALIAS] == self.name:
                    nodeAddress = data[NODE_IP]
                    break

        info = {
            'name': self.name,
            'rank': self.rank,
            'view': self.viewNo,
            'creationDate': self.created,
            'ledger_dir': self.ledger_dir,
            'keys_dir': self.keys_dir,
            'genesis_dir': self.genesis_dir,
            'plugins_dir': self.plugins_dir,
            'node_info_dir': self.node_info_dir,
            'portN': self.nodestack.ha[1],
            'portC': self.clientstack.ha[1],
            'address': nodeAddress
        }
        return info

    def logNodeInfo(self):
        """
        Print the node's info to log for the REST backend to read.
        """
        self.nodeInfo['data'] = self.collectNodeInfo()

        with closing(open(os.path.join(self.ledger_dir, 'node_info'), 'w')) \
                as logNodeInfoFile:
            logNodeInfoFile.write(json.dumps(self.nodeInfo['data']))

    def add_observer(self, observer_remote_id: str,
                     observer_policy_type: ObserverSyncPolicyType):
        self._observable.add_observer(
            observer_remote_id, observer_policy_type)

    def remove_observer(self, observer_remote_id):
        self._observable.remove_observer(observer_remote_id)

    def get_observers(self, observer_policy_type: ObserverSyncPolicyType):
        return self._observable.get_observers(observer_policy_type)
