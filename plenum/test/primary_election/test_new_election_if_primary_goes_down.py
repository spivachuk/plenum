import pytest

from plenum.common.eventually import eventually
from plenum.test.conftest import patchPluginManager, txnPoolNodesLooper, \
    tdirWithPoolTxns, tdirWithDomainTxns, tconf, poolTxnNodeNames, \
    allPluginsPath, tdirWithNodeKeepInited, testNodeClass
from plenum.test.helper import stopNodes
from plenum.test.test_node import checkPoolReady, checkProtocolInstanceSetup, \
    checkNodesConnected


def primaries(nodes):
    return {N for N in nodes if N.hasPrimary}


@pytest.fixture
def txnPoolNodes(patchPluginManager,
                 txnPoolNodesLooper,
                 tdirWithPoolTxns,
                 tdirWithDomainTxns,
                 tconf,
                 poolTxnNodeNames,
                 allPluginsPath,
                 tdirWithNodeKeepInited,
                 testNodeClass):

    return [testNodeClass(name, basedirpath=tdirWithPoolTxns,
                          config=tconf, pluginPaths=allPluginsPath)
            for name in poolTxnNodeNames]


@pytest.fixture
def electWithClearWinner(txnPoolNodes):
    A, B, C, D = txnPoolNodes
    for node in [B, C, D]:
        node.delaySelfNomination(4)


def test_new_election_if_primary_goes_down(txnPoolNodes, txnPoolNodesLooper,
                                           electWithClearWinner):
    nodes = txnPoolNodes
    looper = txnPoolNodesLooper
    A, B, C, D = nodes

    for node in nodes:
        txnPoolNodesLooper.add(node)
    txnPoolNodesLooper.run(checkNodesConnected(nodes))
    checkPoolReady(looper, nodes)
    checkProtocolInstanceSetup(looper, nodes, timeout=10)

    assert 0 == A.viewNo
    assert 0 == B.viewNo
    assert 0 == C.viewNo
    assert 0 == D.viewNo
    primariesBefore = primaries(nodes)
    assert 2 == len(primariesBefore)
    assert A in primariesBefore

    stopNodes([A], looper)

    def assertNewPrimariesElected():
        assert 1 == B.viewNo
        assert 1 == C.viewNo
        assert 1 == D.viewNo
        primariesAfter = primaries([B, C, D])
        assert 2 == len(primariesAfter)

    looper.run(eventually(assertNewPrimariesElected, retryWait=1, timeout=10))
