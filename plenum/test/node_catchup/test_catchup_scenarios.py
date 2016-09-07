import pytest
from plenum.common.startable import Mode
from plenum.common.util import getlogger
from plenum.test.eventually import eventually
from plenum.test.helper import checkNodesConnected, sendRandomRequests, crDelay
from plenum.test.node_catchup.helper import \
    ensureClientConnectedToNodesAndPoolLedgerSame

logger = getlogger()

txnCount = 10


@pytest.fixture("module")
def nodeStashingOrderedRequests(txnPoolNodeSet, nodeCreatedAfterSomeTxns):
    looper, newNode, client, _ = nodeCreatedAfterSomeTxns
    for node in txnPoolNodeSet:
        node.nodeIbStasher.delay(crDelay(5))
    txnPoolNodeSet.append(newNode)
    ensureClientConnectedToNodesAndPoolLedgerSame(looper, client,
                                                  *txnPoolNodeSet[:-1])
    sendRandomRequests(client, 10)
    looper.run(eventually(checkNodesConnected, txnPoolNodeSet, retryWait=1,
                          timeout=15))

    def stashing():
        assert newNode.mode != Mode.participating
        assert len(newNode.stashedOrderedReqs) > 0
        assert len(newNode.reqsFromCatchupReplies) > 0

    looper.run(eventually(stashing, retryWait=1, timeout=20))


@pytest.mark.skipif(True, reason="Incomplete")
def testNodeNotProcessingOrderedReqsWhileCatchingUp(nodeStashingOrderedRequests):
    """
    Check that node does not execute requests while catching up
    :return:
    """
    pass


@pytest.mark.skipif(True, reason="Incomplete")
def testExecutedInOrderAfterCatchingUp(txnPoolNodeSet,
                                       nodeStashingOrderedRequests):
    """
    After catching up, while executing check for already see client id and
    request id., maintain a list of seen client id and request ids, the node
    while catching up keeps track of seen client ids and request id
    Reset monitor after executing all stashed requests so no view change can
    be proposed
    :return:
    """
    newNode = txnPoolNodeSet[-1]