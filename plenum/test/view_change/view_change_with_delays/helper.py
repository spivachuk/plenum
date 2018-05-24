from plenum.common.util import max_3PC_key, getNoInstances
from plenum.test import waits
from plenum.test.delayers import vcd_delay, icDelay, cDelay
from plenum.test.helper import sdk_send_random_request, sdk_get_reply
from plenum.test.stasher import delay_rules
from stp_core.loop.eventually import eventually


def last_prepared_certificate(nodes):
    """
    Find last prepared certificate in pool.
    When we don't have any request ordered in new view last_prepared_certificate_in_view()
    returns None, but in order to ease maths (like being able to use max_3PC_key, or calculating
    next expected 3PC key) this value is replaced with (view_no, 0).
    """

    def patched_last_prepared_certificate(n):
        result = n.master_replica.last_prepared_certificate_in_view()
        if result is None:
            result = (n.master_replica.viewNo, 0)
        return result

    return max_3PC_key(patched_last_prepared_certificate(n) for n in nodes)


def check_last_prepared_certificate(nodes, num):
    # Check that last_prepared_certificate reaches some 3PC key on N-f nodes
    assert sum(1 for n in nodes if n.master_replica.last_prepared_certificate_in_view() == num) >= 3


def check_view_change_done(nodes, view_no):
    # Check that view change is done and view_no is not less than target
    for n in nodes:
        assert n.master_replica.viewNo >= view_no
        assert n.master_replica.last_prepared_before_view_change is None


def do_view_change_with_commits_after_view_change_on_one_node(slow_node, nodes, looper,
                                                              sdk_pool_handle, sdk_wallet_client):
    fast_nodes = [n for n in nodes if n != slow_node]

    # Get last prepared certificate in pool
    lpc = last_prepared_certificate(nodes)
    # Get pool current view no
    view_no = lpc[0]

    with delay_rules(slow_node, vcd_delay()):
        with delay_rules(slow_node, icDelay()):
            with delay_rules(nodes, cDelay()):
                # Send request
                request = sdk_send_random_request(looper, sdk_pool_handle, sdk_wallet_client)

                # Wait until this request is prepared on N-f nodes
                looper.run(eventually(check_last_prepared_certificate, nodes, (lpc[0], lpc[1] + 1)))

                # Trigger view change
                for n in nodes:
                    n.view_changer.on_master_degradation()

                # Wait until view change is completed on fast nodes
                looper.run(eventually(check_view_change_done, fast_nodes, view_no + 1, timeout=60))

            # Now all the nodes receive commits
            looper.runFor(waits.expectedOrderingTime(getNoInstances(len(nodes))))

        # Now slow node receives
        looper.runFor(waits.expectedPoolConsistencyProof(len(nodes)) +
                      waits.expectedPoolCatchupTime(len(nodes)) +
                      waits.expectedPoolElectionTimeout(len(nodes)))

    # Finish request gracefully
    sdk_get_reply(looper, request)
