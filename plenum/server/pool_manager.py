from typing import Dict, Tuple
from functools import lru_cache

from copy import deepcopy
from ledger.util import F
from plenum.common.txn_util import updateGenesisPoolTxnFile
from raet.raeting import AutoMode

from plenum.common.exceptions import UnsupportedOperation, \
    UnauthorizedClientRequest

from plenum.common.stack_manager import TxnStackManager

from plenum.common.types import HA, f, Reply
from plenum.common.txn import TXN_TYPE, NODE, TARGET_NYM, DATA, ALIAS, \
    POOL_TXN_TYPES, NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT, VERKEY, SERVICES, \
    VALIDATOR
from plenum.common.log import getlogger

from plenum.common.types import NodeDetail, CLIENT_STACK_SUFFIX


logger = getlogger()


class PoolManager:
    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        """
        Returns a tuple(nodestack, clientstack, nodeReg)
        """
        raise NotImplementedError

    @property
    def merkleRootHash(self):
        raise NotImplementedError

    @property
    def txnSeqNo(self):
        raise NotImplementedError


class HasPoolManager:
    # noinspection PyUnresolvedReferences, PyTypeChecker
    def __init__(self, nodeRegistry=None, ha=None, cliname=None, cliha=None):
        if not nodeRegistry:
            self.poolManager = TxnPoolManager(self, ha=ha, cliname=cliname,
                                              cliha=cliha)
            for types in POOL_TXN_TYPES:
                self.requestExecuter[types] = \
                    self.poolManager.executePoolTxnRequest
        else:
            self.poolManager = RegistryPoolManager(self.name, self.basedirpath,
                                                   nodeRegistry, ha, cliname,
                                                   cliha)


class TxnPoolManager(PoolManager, TxnStackManager):
    def __init__(self, node, ha=None, cliname=None, cliha=None):
        self.node = node
        self.name = node.name
        self.config = node.config
        self.basedirpath = node.basedirpath
        self._ledger = None
        TxnStackManager.__init__(self, self.name, self.basedirpath, isNode=True)
        self.nstack, self.cstack, self.nodeReg, self.cliNodeReg = \
            self.getStackParamsAndNodeReg(self.name, self.basedirpath, ha=ha,
                                          cliname=cliname, cliha=cliha)

    @property
    def hasLedger(self):
        return self.node.hasFile(self.ledgerFile)

    @property
    def ledgerLocation(self):
        return self.node.dataLocation

    @property
    def ledgerFile(self):
        return self.config.poolTransactionsFile

    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        nodeReg, cliNodeReg, nodeKeys = self.parseLedgerForHaAndKeys(self.ledger)

        self.addRemoteKeysFromLedger(nodeKeys)

        # If node name was not found in the pool transactions file
        if not ha:
            ha = nodeReg[name]

        nstack = dict(name=name,
                      ha=HA('0.0.0.0', ha[1]),
                      main=True,
                      auto=AutoMode.never)
        nodeReg[name] = HA(*ha)

        cliname = cliname or (name + CLIENT_STACK_SUFFIX)
        if not cliha:
            cliha = cliNodeReg[cliname]
        cstack = dict(name=cliname or (name + CLIENT_STACK_SUFFIX),
                      ha=HA('0.0.0.0', cliha[1]),
                      main=True,
                      auto=AutoMode.always)
        cliNodeReg[cliname] = HA(*cliha)

        if basedirpath:
            nstack['basedirpath'] = basedirpath
            cstack['basedirpath'] = basedirpath

        return nstack, cstack, nodeReg, cliNodeReg

    def executePoolTxnRequest(self, ppTime, req):
        """
        Execute a transaction that involves consensus pool management, like
        adding a node, client or a steward.

        :param ppTime: PrePrepare request time
        :param req: request
        """
        reply = self.node.generateReply(ppTime, req)
        op = req.operation
        reply.result.update(op)
        merkleProof = self.node.appendResultToLedger(reply.result)
        txn = deepcopy(reply.result)
        txn[F.seqNo.name] = merkleProof[F.seqNo.name]
        self.onPoolMembershipChange(txn)
        reply.result.update(merkleProof)
        self.node.sendReplyToClient(reply, req.key)

    def getReplyFor(self, request):
        return self.node.getReplyFromLedger(self.ledger, request)

    def onPoolMembershipChange(self, txn):
        if txn[TXN_TYPE] == NODE:
            nodeName = txn[DATA][ALIAS]
            nodeNym = txn[TARGET_NYM]

            def _updateNode(txn):
                if {NODE_IP, NODE_PORT, CLIENT_IP, CLIENT_PORT}. \
                        intersection(set(txn[DATA].keys())):
                    self.nodeHaChanged(txn)
                if VERKEY in txn:
                    self.nodeKeysChanged(txn)
                if SERVICES in txn[DATA]:
                    self.nodeServicesChanged(txn)

            if nodeName in self.nodeReg:
                # The node was already part of the pool so update
                _updateNode(txn)
            else:
                seqNos, info = self.getNodeInfoFromLedger(nodeNym)
                if len(seqNos) == 1:
                    # Since only one transaction has been made, this is a new
                    # node transaction
                    self.addNewNodeAndConnect(txn)
                else:
                    self.node.nodeReg[nodeName] = HA(info[DATA][NODE_IP],
                                                     info[DATA][NODE_PORT])
                    self.node.cliNodeReg[nodeName] = HA(info[DATA][CLIENT_IP],
                                                        info[DATA][CLIENT_PORT])
                    _updateNode(txn)

            self.node.sendPoolInfoToClients(txn)
            if self.config.UpdateGenesisPoolTxnFile:
                updateGenesisPoolTxnFile(self.config.baseDir,
                                         self.config.poolTransactionsFile, txn)

    def addNewNodeAndConnect(self, txn):
        nodeName = txn[DATA][ALIAS]
        if nodeName == self.name:
            logger.debug("{} not adding itself to node registry".
                         format(self.name))
            return
        self.connectNewRemote(txn, nodeName, self.node)
        self.node.newNodeJoined(txn)

    def nodeHaChanged(self, txn):
        nodeNym = txn[TARGET_NYM]
        nodeName = self.getNodeName(nodeNym)
        # TODO: Check if new HA is same as old HA and only update if
        # new HA is different.
        if nodeName == self.name:
            logger.debug("{} clearing local data in keep".
                         format(self.node.nodestack.name))
            self.node.nodestack.keep.clearLocalData()
            logger.debug("{} clearing local data in keep".
                         format(self.node.clientstack.name))
            self.node.clientstack.keep.clearLocalData()
        else:
            rid = self.stackHaChanged(txn, nodeName, self.node)
            if rid:
                self.node.nodestack.outBoxes.pop(rid, None)
            # self.node.sendPoolInfoToClients(txn)
        self.node.startViewChangeIfNeeded([nodeName])

    def nodeKeysChanged(self, txn):
        # TODO: if the node whose keys are being changed is primary for any
        # protocol instance, then we should trigger an election for that
        # protocol instance. For doing that, for every replica of that
        # protocol instance, `_primaryName` as None, and then the node should
        # call its `decidePrimaries`.
        nodeNym = txn[TARGET_NYM]
        nodeName = self.getNodeName(nodeNym)
        # TODO: Check if new keys are same as old keys and only update if
        # new keys are different.
        if nodeName == self.name:
            # TODO: Why?
            logger.debug("{} not changing itself's keep".
                         format(self.name))
            return
        else:
            rid = self.stackKeysChanged(txn, nodeName, self.node)
            if rid:
                self.node.nodestack.outBoxes.pop(rid, None)
            # self.node.sendPoolInfoToClients(txn)
        self.node.startViewChangeIfNeeded([nodeName])

    def nodeServicesChanged(self, txn):
        nodeNym = txn[TARGET_NYM]
        _, nodeInfo = self.getNodeInfoFromLedger(nodeNym)
        nodeName = nodeInfo[DATA][ALIAS]
        oldServices = set(nodeInfo[DATA][SERVICES])
        newServices = set(txn[DATA][SERVICES])
        if oldServices == newServices:
            logger.debug("Node {} not changing {} since it is same as existing"
                         .format(nodeNym, SERVICES))
            return
        else:
            if self.name != nodeName:
                if VALIDATOR in newServices.difference(oldServices):
                    # If validator service is enabled
                    self.updateNodeTxns(nodeInfo, txn)
                    self.connectNewRemote(nodeInfo, nodeName, self.node)

                if VALIDATOR in oldServices.difference(newServices):
                    # If validator service is disabled
                    del self.node.nodeReg[nodeName]
                    del self.node.cliNodeReg[nodeName + CLIENT_STACK_SUFFIX]
                    rid = self.node.nodestack.removeRemoteByName(nodeName)
                    if rid:
                        self.node.nodestack.outBoxes.pop(rid, None)
                    self.node.nodeLeft(txn)
            self.node.startViewChangeIfNeeded([nodeName])

    def getNodeName(self, nym):
        # Assuming ALIAS does not change
        _, nodeTxn = self.getNodeInfoFromLedger(nym)
        return nodeTxn[DATA][ALIAS]

    def checkValidOperation(self, operation):
        checks = []
        if operation[TXN_TYPE] == NODE:
            checks.append(DATA in operation and isinstance(operation[DATA], dict))
        return all(checks)

    def checkRequestAuthorized(self, request):
        typ = request.operation.get(TXN_TYPE)
        error = None
        if typ == NODE:
            nodeNym = request.operation.get(TARGET_NYM)
            if self.nodeExistsInLedger(nodeNym):
                error = self.authErrorWhileUpdatingNode(request)
            else:
                error = self.authErrorWhileAddingNode(request)
        if error:
            raise UnauthorizedClientRequest(request.identifier, request.reqId,
                                            error)

    def authErrorWhileAddingNode(self, request):
        origin = request.identifier
        operation = request.operation
        isSteward = self.node.secondaryStorage.isSteward(origin)
        data = operation.get(DATA, {})
        invalidData = self._validateNodeData(data)
        if invalidData:
            return invalidData

        if not isSteward:
            return "{} is not a steward so cannot add a new node".format(origin)

        for txn in self.ledger.getAllTxn().values():
            if txn[TXN_TYPE] == NODE:
                if txn[f.IDENTIFIER.nm] == origin:
                    return "{} already has a node with name {}". \
                        format(origin, txn[DATA][ALIAS])

        if self.isNodeDataConflicting(data, operation.get(TARGET_NYM)):
            return "existing data has conflicts with " \
                   "request data {}".format(operation.get(DATA))

    @lru_cache(maxsize=64)
    def isStewardOfNode(self, stewardNym, nodeNym):
        for txn in self.ledger.getAllTxn().values():
            if txn[TXN_TYPE] == NODE and \
                            txn[TARGET_NYM] == nodeNym and \
                            txn[f.IDENTIFIER.nm] == stewardNym:
                return True
        return False

    @staticmethod
    def _validateNodeData(data):
        if data.get(NODE_IP, "nodeip") == data.get(CLIENT_IP, "clientip") and \
                        data.get(NODE_PORT, "nodeport") == data.get(CLIENT_PORT, "clientport"):
            return "node and client ha can't be same"

    def authErrorWhileUpdatingNode(self, request):
        origin = request.identifier
        operation = request.operation
        isSteward = self.node.secondaryStorage.isSteward(origin)
        data = operation.get(DATA, {})
        invalidData = self._validateNodeData(data)
        if invalidData:
            return invalidData
        if not isSteward:
            return "{} is not a steward so cannot update a node".format(origin)
        nodeNym = operation.get(TARGET_NYM)
        if not self.isStewardOfNode(origin, nodeNym):
            return "{} is not a steward of node {}".format(origin, nodeNym)
        for txn in self.ledger.getAllTxn().values():
            if txn[TXN_TYPE] == NODE and nodeNym == txn[TARGET_NYM]:
                if txn[DATA] == operation.get(DATA, {}):
                    return "node already has the same data as requested"
        if self.isNodeDataConflicting(data, nodeNym):
            return "existing data has conflicts with " \
                   "request data {}".format(operation.get(DATA))

    @property
    def merkleRootHash(self):
        return self.ledger.root_hash

    @property
    def txnSeqNo(self):
        return self.ledger.seqNo

    def getNodeData(self, nym):
        _, nodeTxn = self.getNodeInfoFromLedger(nym)
        return nodeTxn[DATA]

    def _checkAgainstOtherNodePoolTxns(self, data, existingNodeTxn):
        otherNodeData = existingNodeTxn[DATA]
        for (ip, port) in [(NODE_IP, NODE_PORT),
                           (CLIENT_IP, CLIENT_PORT)]:
            if (otherNodeData.get(ip), otherNodeData.get(port)) == (
            data.get(ip), data.get(port)):
                return True

        if otherNodeData.get(ALIAS) == data.get(ALIAS):
            return True

    def _checkAgainstSameNodePoolTxns(self, data, existingNodeTxn):
        sameNodeData = existingNodeTxn[DATA]
        if sameNodeData.get(ALIAS) != data.get(ALIAS):
            return True

    def isNodeDataConflicting(self, data, nodeNym=None):
        for existingNodeTxn in [t for t in self.ledger.getAllTxn().values()
                    if t[TXN_TYPE] == NODE]:
            if not nodeNym or nodeNym != existingNodeTxn[TARGET_NYM]:
                conflictFound = self._checkAgainstOtherNodePoolTxns(data, existingNodeTxn)
                if conflictFound:
                    return conflictFound
            if nodeNym and nodeNym == existingNodeTxn[TARGET_NYM]:
                conflictFound = self._checkAgainstSameNodePoolTxns(data, existingNodeTxn)
                if conflictFound:
                    return conflictFound

class RegistryPoolManager(PoolManager):
    def __init__(self, name, basedirpath, nodeRegistry, ha, cliname, cliha):

        self.nstack, self.cstack, self.nodeReg, self.cliNodeReg = \
            self.getStackParamsAndNodeReg(name=name, basedirpath=basedirpath,
                                          nodeRegistry=nodeRegistry, ha=ha,
                                          cliname=cliname, cliha=cliha)

    def getStackParamsAndNodeReg(self, name, basedirpath, nodeRegistry=None,
                                 ha=None, cliname=None, cliha=None):
        nstack, nodeReg, cliNodeReg = self.getNodeStackParams(name,
                                                              nodeRegistry,
                                                              ha,
                                                              basedirpath)

        cstack = self.getClientStackParams(name, nodeRegistry,
                                           cliname=cliname, cliha=cliha,
                                           basedirpath=basedirpath)

        return nstack, cstack, nodeReg, cliNodeReg

    @staticmethod
    def getNodeStackParams(name, nodeRegistry: Dict[str, HA],
                           ha: HA = None,
                           basedirpath: str = None) -> Tuple[dict, dict, dict]:
        """
        Return tuple(nodeStack params, nodeReg)
        """
        me = nodeRegistry[name]
        if isinstance(me, NodeDetail):
            sha = me.ha
            nodeReg = {k: v.ha for k, v in nodeRegistry.items()}
        else:
            sha = me if isinstance(me, HA) else HA(*me[0])
            nodeReg = {k: v if isinstance(v, HA) else HA(*v[0])
                       for k, v in nodeRegistry.items()}
        if not ha:  # pull it from the registry
            ha = sha

        cliNodeReg = {r.cliname: r.cliha for r in nodeRegistry.values()}

        nstack = dict(name=name,
                      ha=ha,
                      main=True,
                      auto=AutoMode.never)

        if basedirpath:
            nstack['basedirpath'] = basedirpath

        return nstack, nodeReg, cliNodeReg

    @staticmethod
    def getClientStackParams(name, nodeRegistry: Dict[str, HA], cliname,
                             cliha, basedirpath) -> dict:
        """
        Return clientStack params
        """
        me = nodeRegistry[name]
        if isinstance(me, NodeDetail):
            sha = me.ha
            scliname = me.cliname
            scliha = me.cliha
        else:
            sha = me if isinstance(me, HA) else HA(*me[0])
            scliname = None
            scliha = None

        if not cliname:  # default to the name plus the suffix
            cliname = scliname if scliname else name + CLIENT_STACK_SUFFIX
        if not cliha:  # default to same ip, port + 1
            cliha = scliha if scliha else HA(sha[0], sha[1] + 1)

        cstack = dict(name=cliname,
                      ha=cliha,
                      main=True,
                      auto=AutoMode.always)

        if basedirpath:
            cstack['basedirpath'] = basedirpath

        return cstack

    @property
    def merkleRootHash(self):
        raise UnsupportedOperation

    @property
    def txnSeqNo(self):
        raise UnsupportedOperation
