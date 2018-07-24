import functools
import json

import pytest

from plenum.common.exceptions import RequestRejectedException
from plenum.common.request import Request
from plenum.common.types import f, OPERATION
from plenum.common.util import randomString
from stp_core.loop.eventually import eventually

from plenum.common.constants import DOMAIN_LEDGER_ID, STEWARD_STRING

from plenum.test.pool_transactions.helper import prepare_nym_request, \
    sdk_sign_and_send_prepared_request
from plenum.test import waits
from plenum.test.helper import sdk_send_random_and_check, \
    sdk_get_and_check_replies, get_key_from_req

from stp_core.common.log import Logger
import logging


Logger.setLogLevel(logging.NOTSET)


ERORR_MSG = "something went wrong"

whitelist = [ERORR_MSG]

def testLoggingTxnStateForValidRequest(
        looper, logsearch, txnPoolNodeSet,
        sdk_pool_handle, sdk_wallet_client):
    logsPropagate, _ = logsearch(files=['propagator.py'], funcs=['propagate'],
                                 msgs=['propagating.*request.*from client'])
    logsOrdered, _ = logsearch(files=['replica.py'], funcs=['order_3pc_key'], msgs=['ordered batch request'])
    logsCommited, _ = logsearch(files=['node.py'], funcs=['executeBatch'], msgs=['committed batch request'])

    reqs = sdk_send_random_and_check(looper, txnPoolNodeSet, sdk_pool_handle,
                                     sdk_wallet_client, 1)
    req, _ = reqs[0]

    key = get_key_from_req(req)
    assert any(key in record.getMessage() for record in logsPropagate)
    assert any(key in record.getMessage() for record in logsOrdered)
    assert any(key in record.getMessage() for record in logsCommited)


def testLoggingTxnStateForInvalidRequest(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_client, logsearch):
    logsPropagate, _ = logsearch(files=['propagator.py'], funcs=['propagate'],
                                 msgs=['propagating.*request.*from client'])
    logsReject, _ = logsearch(files=['replica.py'], funcs=['processReqDuringBatch'],
                              msgs=['encountered exception.*while processing.*will reject'])

    seed = randomString(32)
    wh, _ = sdk_wallet_client

    nym_request, _ = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_client, seed,
                            "name", STEWARD_STRING))

    request_couple = sdk_sign_and_send_prepared_request(looper, sdk_wallet_client,
                                                        sdk_pool_handle, nym_request)

    with pytest.raises(RequestRejectedException) as e:
        sdk_get_and_check_replies(looper, [request_couple])

    assert 'Only Steward is allowed to do these transactions' in e._excinfo[1].args[0]
    request = json.loads(nym_request)
    req_id = str(request[f.REQ_ID.nm])
    digest = get_key_from_req(request)
    assert any(digest in record.getMessage() for record in logsPropagate)
    assert any(req_id in record.getMessage() for record in logsReject)


def testLoggingTxnStateWhenCommitFails(
        looper, txnPoolNodeSet, sdk_pool_handle, sdk_wallet_steward, logsearch):
    logsPropagate, _ = logsearch(files=['propagator.py'], funcs=['propagate'],
                                 msgs=['propagating.*request.*from client'])
    logsOrdered, _ = logsearch(files=['replica.py'], funcs=['order_3pc_key'], msgs=['ordered batch request'])
    logsCommitFail, _ = logsearch(files=['node.py'], funcs=['executeBatch'],
                                  msgs=['commit failed for batch request'])

    seed = randomString(32)
    wh, _ = sdk_wallet_steward

    nym_request, _ = looper.loop.run_until_complete(
        prepare_nym_request(sdk_wallet_steward, seed,
                            "name", None))

    sdk_sign_and_send_prepared_request(looper, sdk_wallet_steward,
                                       sdk_pool_handle, nym_request)

    class SomeError(Exception):
        pass

    def commitPatched(node, commitOrig, *args, **kwargs):
        req_handler = node.get_req_handler(ledger_id=DOMAIN_LEDGER_ID)
        req_handler.commit = commitOrig
        raise SomeError(ERORR_MSG)

    excCounter = 0

    def executeBatchPatched(node, executeBatchOrig, *args, **kwargs):
        nonlocal excCounter
        try:
            executeBatchOrig(*args, **kwargs)
        except SomeError:
            excCounter += 1
            node.executeBatch = executeBatchOrig
            pass

    def checkSufficientExceptionsHappend():
        assert excCounter == len(txnPoolNodeSet)
        return

    for node in txnPoolNodeSet:
        req_handler = node.get_req_handler(ledger_id=DOMAIN_LEDGER_ID)
        req_handler.commit = functools.partial(
            commitPatched, node, req_handler.commit
        )
        node.executeBatch = functools.partial(
            executeBatchPatched, node, node.executeBatch
        )

    timeout = waits.expectedTransactionExecutionTime(len(txnPoolNodeSet))
    looper.run(
        eventually(checkSufficientExceptionsHappend,
                   retryWait=1, timeout=timeout))

    request = json.loads(nym_request)
    digest = get_key_from_req(request)
    assert any(digest in record.getMessage() for record in logsPropagate)
    assert any(digest in record.getMessage() for record in logsOrdered)
    assert any(digest in record.getMessage() for record in logsCommitFail)
    assert any(ERORR_MSG in record.getMessage() for record in logsCommitFail)
