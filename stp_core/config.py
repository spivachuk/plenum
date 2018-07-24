import os

import sys

import logging

baseDir = os.getcwd()

# Log configuration
logRotationBackupCount = 150
logRotationMaxBytes = 100 * 1024 * 1024
logRotationCompression = "xz"
logFormat = '{asctime:s}|{levelname:s}|{filename:s}|{message:s}'
logFormatStyle = '{'

logLevel = logging.NOTSET
enableStdOutLogging = True

RETRY_TIMEOUT_NOT_RESTRICTED = 6
RETRY_TIMEOUT_RESTRICTED = 15
MAX_RECONNECT_RETRY_ON_SAME_SOCKET = 1

# Enables/disables debug mode for Looper class
LOOPER_DEBUG = False


# Zeromq configuration
DEFAULT_LISTENER_QUOTA = 100
DEFAULT_SENDER_QUOTA = 100
KEEPALIVE_INTVL = 1     # seconds
KEEPALIVE_IDLE = 20     # seconds
KEEPALIVE_CNT = 10
MAX_SOCKETS = 16384 if sys.platform != 'win32' else None
ENABLE_HEARTBEATS = False
HEARTBEAT_FREQ = 5      # seconds
ZMQ_CLIENT_QUEUE_SIZE = 3000  # messages (0 - no limit)
ZMQ_NODE_QUEUE_SIZE = 20000  # messages (0 - no limit)


# All messages exceeding the limit will be rejected without processing
MSG_LEN_LIMIT = 768 * 1024
