from stp_zmq.zstack import ZStack
from typing import Dict, Callable
from stp_core.network.auth_mode import AuthMode


class SimpleZStack(ZStack):

    def __init__(self,
                 stackParams: Dict,
                 msgHandler: Callable,
                 seed=None,
                 onlyListener=False,
                 sighex: str=None,
                 config=None,
                 msgRejectHandler=None,
                 create_listener_monitor=False):

        # TODO: sighex is unused as of now, remove once test is removed or
        # maybe use sighex to generate all keys, DECISION DEFERRED

        self.stackParams = stackParams
        self.msgHandler = msgHandler

        # TODO: Ignoring `main` param as of now which determines
        # if the stack will have a listener socket or not.
        name = stackParams['name']
        ha = stackParams['ha']
        basedirpath = stackParams['basedirpath']
        queue_size = stackParams['queue_size'] if 'queue_size' in stackParams else 0

        auto = stackParams.pop('auth_mode', None)
        restricted = auto != AuthMode.ALLOW_ANY.value
        super().__init__(name,
                         ha,
                         basedirpath,
                         msgHandler=self.msgHandler,
                         restricted=restricted,
                         seed=seed,
                         onlyListener=onlyListener,
                         config=config,
                         msgRejectHandler=msgRejectHandler,
                         queue_size=queue_size,
                         create_listener_monitor=create_listener_monitor)
