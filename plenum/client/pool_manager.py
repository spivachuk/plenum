import collections
import json

from plenum.common.txn_util import get_payload_data, get_seq_no, get_type
from stp_core.network.exceptions import RemoteNotFound

from plenum.common.stack_manager import TxnStackManager
from plenum.common.constants import NODE, ALIAS, DATA, TARGET_NYM, NODE_IP,\
    NODE_PORT, CLIENT_IP, CLIENT_PORT, VERKEY, SERVICES, VALIDATOR, CLIENT_STACK_SUFFIX
from plenum.common.types import f, HA
from plenum.common.messages.node_messages import PoolLedgerTxns
from plenum.common.util import getMaxFailures
from stp_core.common.log import getlogger
from plenum.common.tools import lazy_field

logger = getlogger()
t = f.TXN.nm


class HasPoolManager(TxnStackManager):
    # noinspection PyUnresolvedReferences
    def __init__(self):
        self._ledgerFile = None
        TxnStackManager.__init__(self, self.name, self.genesis_dir, self.keys_dir,
                                 isNode=False)
        _, cliNodeReg, nodeKeys = self.parseLedgerForHaAndKeys(self.ledger)
        self.nodeReg = cliNodeReg
        self.addRemoteKeysFromLedger(nodeKeys)
        # Temporary place for keeping node transactions while this client is
        # discovering. Key is sequence number and value is dictionary where key
        # is the name of the node and value is the transaction. Transaction
        # should be applied in increasing order of sequence numbers.
        self.tempNodeTxns = {}  # type: Dict[int, Dict[str, Dict]]

    def poolTxnReceived(self, msg: PoolLedgerTxns, frm):
        global t
        logger.debug("{} received pool txn {} from {}".format(self, msg, frm))
        txn = getattr(msg, t)
        seqNo = get_seq_no(txn)
        if seqNo not in self.tempNodeTxns:
            self.tempNodeTxns[seqNo] = {}
        self.tempNodeTxns[seqNo][frm] = txn
        # If this is the next sequence number that should go into ledger, then
        # check if there are enough same transactions from different nodes
        if (seqNo - self.ledger.size) == 1:
            f = getMaxFailures(len(self.nodeReg))
            if len(self.tempNodeTxns[seqNo]) > f:
                # TODO: Shouldnt this use `checkIfMoreThanFSameItems`
                txns = [item for item, count in
                        collections.Counter(
                            [json.dumps(_t, sort_keys=True)
                             for _t in self.tempNodeTxns[seqNo].values()]
                        ).items() if count > f]
                if len(txns) > 0:
                    txn = json.loads(txns[0])
                    self.addToLedger(txn)
                    self.tempNodeTxns.pop(seqNo)
                else:
                    logger.error("{} has not got enough similar node "
                                 "transactions".format(self))

    # noinspection PyUnresolvedReferences
    def processPoolTxn(self, txn):
        logger.debug("{} processing pool txn {} ".format(self, txn))
        typ = get_type(txn)
        txn_data = get_payload_data(txn)

        if typ == NODE:
            remoteName = txn_data[DATA][ALIAS] + CLIENT_STACK_SUFFIX
            nodeName = txn_data[DATA][ALIAS]
            nodeNym = txn_data[TARGET_NYM]

            def _update(txn_data):
                if {NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT}.\
                        intersection(set(txn_data[DATA].keys())):
                    self.stackHaChanged(txn_data, remoteName, self)
                if VERKEY in txn_data:
                    self.stackKeysChanged(txn_data, remoteName, self)
                if SERVICES in txn_data[DATA]:
                    self.nodeServicesChanged(txn_data)
                    self.setPoolParams()

            if nodeName in self.nodeReg:
                # The node was already part of the pool so update
                _update(txn_data)
            else:
                seqNos, info = self.getNodeInfoFromLedger(nodeNym)
                if len(seqNos) == 1:
                    # Since only one transaction has been made, this is a new
                    # node transactions
                    self.connectNewRemote(txn_data, remoteName, self)
                    self.setPoolParams()
                else:
                    self.nodeReg[nodeName + CLIENT_STACK_SUFFIX] = HA(
                        info[DATA][CLIENT_IP], info[DATA][CLIENT_PORT])
                    _update(txn_data)
        else:
            logger.error("{} received unknown txn type {} in txn {}"
                         .format(self.name, typ, txn))
            return

    def nodeServicesChanged(self, txn_data):
        nodeNym = txn_data[TARGET_NYM]
        _, nodeInfo = self.getNodeInfoFromLedger(nodeNym)
        remoteName = nodeInfo[DATA][ALIAS] + CLIENT_STACK_SUFFIX
        oldServices = set(nodeInfo[DATA].get(SERVICES, []))
        newServices = set(txn_data[DATA].get(SERVICES, []))
        if oldServices == newServices:
            logger.debug(
                "Client {} not changing {} since it is same as existing"
                .format(nodeNym, SERVICES))
            return
        else:
            if VALIDATOR in newServices.difference(oldServices):
                # If validator service is enabled
                self.updateNodeTxns(nodeInfo, txn_data)
                self.connectNewRemote(nodeInfo, remoteName, self)

            if VALIDATOR in oldServices.difference(newServices):
                # If validator service is disabled
                del self.nodeReg[remoteName]
                try:
                    rid = TxnStackManager.removeRemote(
                        self.nodestack, remoteName)
                    if rid:
                        self.nodestack.outBoxes.pop(rid, None)
                except RemoteNotFound:
                    logger.info('{} did not find remote {} to remove'.format(self, remoteName))

    # noinspection PyUnresolvedReferences
    @property
    def hasLedger(self):
        return self.hasFile(self.ledgerFile)

    @lazy_field
    def ledgerLocation(self):
        # noinspection PyUnresolvedReferences
        return self.dataLocation

    @lazy_field
    def ledgerFile(self):
        # noinspection PyUnresolvedReferences
        return self.config.poolTransactionsFile

    def addToLedger(self, txn):
        logger.debug("{} adding txn {} to pool ledger".format(self, txn))
        self.ledger.append(txn)
        self.processPoolTxn(txn)
