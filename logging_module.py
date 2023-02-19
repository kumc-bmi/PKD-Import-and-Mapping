# Using the code from URL: https://stackoverflow.com/questions/18052778/should-a-python-logger-be-passed-as-parameter
import logging

FILENAME = "PKD_log.log" # Your logfile
LOGFORMAT = "%(message)s" # Your format
DEFAULT_LEVEL = "info" # Your default level, usually set to warning or error for production
LEVELS = {
    'debug':logging.DEBUG,
    'info':logging.INFO,
    'warning':logging.WARNING,
    'error':logging.ERROR,
    'critical':logging.CRITICAL}

def startlogging(filename=FILENAME, level=DEFAULT_LEVEL):
    logging.basicConfig(filename=filename, level=LEVELS[level], format=LOGFORMAT)
