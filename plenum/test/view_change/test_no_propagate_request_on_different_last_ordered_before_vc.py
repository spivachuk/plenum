import pytest
import sys

from plenum.server.node import Node
from plenum.test.delayers import cDelay, pDelay, ppDelay
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_send_random_requests, sdk_get_replies
from plenum.test.stasher import delay_rules
from plenum.test.test_node import ensureElectionsDone
from stp_core.loop.eventually import eventually


def test_no_propagate_request_on_different_last_ordered_on_backup_before_vc(looper, txnPoolNodeSet,
                                                  sdk_pool_handle, sdk_wallet_client):
    ''' Send random request and do view change then fast_nodes (1, 4 - without
    primary backup replicas) are already ordered transaction on master and some backup replica
    and slow_nodes are not on backup replica. Wait ordering on slow_nodes.'''
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    slow_instance = 1
    slow_nodes = txnPoolNodeSet[1:3]
    fast_nodes = [n for n in txnPoolNodeSet if n not in slow_nodes]
    nodes_stashers = [n.nodeIbStasher for n in slow_nodes]
    old_last_ordered = txnPoolNodeSet[0].replicas[slow_instance].last_ordered_3pc
    with delay_rules(nodes_stashers, cDelay(delay=sys.maxsize,
                                            instId=slow_instance)):
        # send one  request
        requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                            sdk_wallet_client, 1)
        sdk_get_replies(looper, requests)
        old_view_no = txnPoolNodeSet[0].viewNo
        looper.run(
            eventually(check_last_ordered,
                       fast_nodes,
                       slow_instance,
                       (old_view_no, old_last_ordered[1] + 1)))
        check_last_ordered(slow_nodes, slow_instance, old_last_ordered)

        # trigger view change on all nodes
        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()

        # wait for view change done on all nodes
        ensureElectionsDone(looper, txnPoolNodeSet)

    looper.run(
        eventually(check_last_ordered,
                   txnPoolNodeSet,
                   slow_instance))
    check_last_ordered(txnPoolNodeSet,
                       txnPoolNodeSet[0].master_replica.instId,
                       (old_last_ordered[0], old_last_ordered[1] + 1))
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    assert all(0 == node.spylog.count(node.request_propagates)
               for node in txnPoolNodeSet)


def test_no_propagate_request_on_different_prepares_on_backup_before_vc(looper, txnPoolNodeSet,
                                                  sdk_pool_handle, sdk_wallet_client):
    ''' Send random request and do view change then fast_nodes (2,3 - with
    primary backup replica) will have prepare or send preprepare on backup
    replicas and slow_nodes are have not and transaction will ordered on all
    master replicas. Check last ordered after view change and after another
    one request.'''
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    slow_instance = 1
    slow_nodes = txnPoolNodeSet[1:3]
    fast_nodes = [n for n in txnPoolNodeSet if n not in slow_nodes]
    nodes_stashers = [n.nodeIbStasher for n in slow_nodes]
    with delay_rules(nodes_stashers, pDelay(delay=sys.maxsize,
                                            instId=slow_instance)):
        with delay_rules(nodes_stashers, ppDelay(delay=sys.maxsize,
                                                 instId=slow_instance)):
            # send one  request
            requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                                sdk_wallet_client, 1)
            sdk_get_replies(looper, requests)
            looper.run(
                eventually(is_prepared,
                           fast_nodes,
                           2,
                           slow_instance))

            # trigger view change on all nodes
            for node in txnPoolNodeSet:
                node.view_changer.on_master_degradation()

            # wait for view change done on all nodes
            ensureElectionsDone(looper, txnPoolNodeSet)

    last_ordered_3pc = fast_nodes[0].replicas[slow_instance].last_ordered_3pc
    for node in txnPoolNodeSet:
        assert last_ordered_3pc == node.replicas[slow_instance].last_ordered_3pc
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    looper.run(
        eventually(check_last_ordered,
                   txnPoolNodeSet,
                   slow_instance,
                   (txnPoolNodeSet[0].viewNo, 1)))
    assert all(0 == node.spylog.count(node.request_propagates)
               for node in txnPoolNodeSet)


def test_no_propagate_request_on_different_last_ordered_on_master_before_vc(looper, txnPoolNodeSet,
                                                  sdk_pool_handle, sdk_wallet_client):
    ''' Send random request and do view change then fast_nodes (1, 4 - without
    primary after next view change) are already ordered transaction on master
    and slow_nodes are not. Check ordering on slow_nodes.'''
    sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                              sdk_wallet_client, 1)
    master_instance = txnPoolNodeSet[0].master_replica.instId
    slow_nodes = txnPoolNodeSet[1:3]
    fast_nodes = [n for n in txnPoolNodeSet if n not in slow_nodes]
    nodes_stashers = [n.nodeIbStasher for n in slow_nodes]
    old_last_ordered = txnPoolNodeSet[0].master_replica.last_ordered_3pc
    with delay_rules(nodes_stashers, cDelay(delay=sys.maxsize)):
        # send one  request
        requests = sdk_send_random_requests(looper, sdk_pool_handle,
                                             sdk_wallet_client, 1)
        last_ordered_for_slow = slow_nodes[0].master_replica.last_ordered_3pc
        old_view_no = txnPoolNodeSet[0].viewNo
        looper.run(
            eventually(check_last_ordered,
                       fast_nodes,
                       master_instance,
                       (old_view_no, old_last_ordered[1] + 1)))

        # trigger view change on all nodes
        for node in txnPoolNodeSet:
            node.view_changer.on_master_degradation()

        # wait for view change done on all nodes
        ensureElectionsDone(looper, txnPoolNodeSet)

    sdk_get_replies(looper, requests)
    looper.run(
        eventually(check_last_ordered,
                   slow_nodes,
                   master_instance,
                   (old_view_no, last_ordered_for_slow[1] + 1)))
    assert all(0 == node.spylog.count(node.request_propagates)
               for node in txnPoolNodeSet)


def is_prepared(nodes: [Node], ppSeqNo, instId):
    for node in nodes:
        replica = node.replicas[instId]
        assert (node.viewNo, ppSeqNo) in replica.prepares or \
               (node.viewNo, ppSeqNo) in replica.sentPrePrepares


def check_last_ordered(nodes: [Node],
                       instId,
                       last_ordered=None):
    if last_ordered is None:
        last_ordered = nodes[0].replicas[instId].last_ordered_3pc
    for node in nodes:
        assert node.replicas[instId].last_ordered_3pc == last_ordered
