import inspect
import logging
import os
import sys

from ioflo.base.consoling import getConsole, Console

from plenum.common.logging.TimeAndSizeRotatingFileHandler \
    import TimeAndSizeRotatingFileHandler
from plenum.common.util import Singleton, adict

TRACE_LOG_LEVEL = 5
DISPLAY_LOG_LEVEL = 25


class CustomAdapter(logging.LoggerAdapter):
    def trace(self, msg, *args, **kwargs):
        self.log(TRACE_LOG_LEVEL, msg, *args, **kwargs)

    def display(self, msg, *args, **kwargs):
        self.log(DISPLAY_LOG_LEVEL, msg, *args, **kwargs)


class CallbackHandler(logging.Handler):
    def __init__(self, typestr, default_tags, callback, override_tags):
        """
        Initialize the handler.
        """
        super().__init__()
        self.callback = callback
        self.tags = default_tags
        self.update_tags(override_tags or {})
        self.typestr = typestr

    def update_tags(self, override_tags):
        self.tags.update(override_tags)

    def emit(self, record):
        """
        Passes the log record back to the CLI for rendering
        """
        should_cb = None
        attr_val = None
        if hasattr(record, self.typestr):
            attr_val = getattr(record, self.typestr)
            should_cb = bool(attr_val)
        if should_cb is None and record.levelno >= logging.INFO:
            should_cb = True
        if hasattr(record, 'tags'):
            for t in record.tags:
                if t in self.tags:
                    if self.tags[t]:
                        should_cb = True
                        continue
                    else:
                        should_cb = False
                        break
        if should_cb:
            self.callback(record, attr_val)


class CliHandler(CallbackHandler):
    def __init__(self, callback, override_tags=None):
        default_tags = {
            "add_replica": True
        }
        super().__init__(typestr="cli",
                         default_tags=default_tags,
                         callback=callback,
                         override_tags=override_tags)


class DemoHandler(CallbackHandler):
    def __init__(self, callback, override_tags=None):
        default_tags = {
            "add_replica": True
        }
        super().__init__(typestr="demo",
                         default_tags=default_tags,
                         callback=callback,
                         override_tags=override_tags)


def getlogger(name=None):
    return Logger().getlogger(name)


class TestingHandler(logging.Handler):
    def __init__(self, tester):
        """
        Initialize the handler.
        """
        super().__init__()
        self.tester = tester

    def emit(self, record):
        """
        Captures a record.
        """
        self.tester(record)


class Logger(metaclass=Singleton):
    def __init__(self, config=None):
        from plenum.common.config_util import getConfig
        # TODO: This should take directory
        self._config = config or getConfig()
        self._addTraceToLogging()
        self._addDisplayToLogging()

        self._handlers = {}
        self._format = logging.Formatter(fmt=self._config.logFormat,
                                         style=self._config.logFormatStyle)

        self._default_raet_verbosity = \
            getRAETLogLevelFromConfig("RAETLogLevel",
                                      Console.Wordage.terse,
                                      self._config)

        self._default_raet_log_file = \
            getRAETLogFilePath("RAETLogFilePath", self._config)

        if self._config.enableStdOutLogging:
            self.enableStdLogging()

        logLevel = logging.INFO
        if hasattr(self._config, "logLevel"):
            logLevel = self._config.logLevel
        self.setLogLevel(logLevel)

    @staticmethod
    def getlogger(name=None):
        if not name:
            curframe = inspect.currentframe()
            calframe = inspect.getouterframes(curframe, 2)
            name = inspect.getmodule(calframe[1][0]).__name__
        logger = logging.getLogger(name)
        return logger

    @staticmethod
    def setLogLevel(log_level):
        logging.root.setLevel(log_level)

    def setupRaet(self, raet_log_level=None, raet_log_file=None):
        console = getConsole()

        verbosity = raet_log_level \
            if raet_log_level is not None \
            else self._default_raet_verbosity
        file = raet_log_file or self._default_raet_log_file

        logging.info("Setting RAET log level {}".format(verbosity),
                     extra={"cli": False})

        console.reinit(verbosity=verbosity, path=file, flushy=True)

    def enableStdLogging(self):
        # only enable if CLI is not
        if 'cli' in self._handlers:
            raise RuntimeError('cannot configure STD logging '
                               'when CLI logging is enabled')
        new = logging.StreamHandler(sys.stdout)
        self._setHandler('std', new)

    def enableCliLogging(self, callback, override_tags=None):
        h = CliHandler(callback, override_tags)
        self._setHandler('cli', h)
        # assumption is there's never a need to have std logging when in CLI
        self._clearHandler('std')

    def enableFileLogging(self, filename):
        d = os.path.dirname(filename)
        if not os.path.exists(d):
            os.makedirs(d)
        new = TimeAndSizeRotatingFileHandler(
            filename,
            when=self._config.logRotationWhen,
            interval=self._config.logRotationInterval,
            backupCount=self._config.logRotationBackupCount,
            utc=True,
            maxBytes=self._config.logRotationMaxBytes)
        self._setHandler('file', new)

    def _setHandler(self, typ: str, new_handler):
        if new_handler.formatter is None:
            new_handler.setFormatter(self._format)

        # assuming indempotence and removing old one first
        self._clearHandler(typ)

        self._handlers[typ] = new_handler
        logging.root.addHandler(new_handler)

    def _clearHandler(self, typ: str):
        old = self._handlers.get(typ)
        if old:
            logging.root.removeHandler(old)

    @staticmethod
    def _addTraceToLogging():
        logging.addLevelName(TRACE_LOG_LEVEL, "TRACE")

        def trace(self, message, *args, **kwargs):
            if self.isEnabledFor(TRACE_LOG_LEVEL):
                self._log(TRACE_LOG_LEVEL, message, args, **kwargs)

        logging.Logger.trace = trace

    @staticmethod
    def _addDisplayToLogging():
        logging.addLevelName(DISPLAY_LOG_LEVEL, "DISPLAY")

        def display(self, message, *args, **kwargs):
            if self.isEnabledFor(DISPLAY_LOG_LEVEL):
                self._log(DISPLAY_LOG_LEVEL, message, args, **kwargs)

        logging.Logger.display = display


def getRAETLogLevelFromConfig(paramName, defaultValue, config):
    try:
        defaultVerbosity = config.__getattribute__(paramName)
        defaultVerbosity = Console.Wordage.__getattribute__(defaultVerbosity)
    except AttributeError:
        defaultVerbosity = defaultValue
        logging.debug("Ignoring RAET log level {} from config and using {} "
                      "instead".format(paramName, defaultValue))
    return defaultVerbosity


def getRAETLogFilePath(paramName, config):
    try:
        filePath = config.__getattribute__(paramName)
    except AttributeError:
        filePath = None
    return filePath


