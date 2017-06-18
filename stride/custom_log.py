# -*- coding: utf-8 -*-
import logging
import os.path
from datetime import datetime
import warnings


class MyFormatter(logging.Formatter):
    """Custom Log Formatter to format time"""
    converter = datetime.fromtimestamp

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s.%03d" % (t, record.msecs)
        return s


def prepare_logger(name, filename, log_dir='../logs'):
    """Generate Logger"""
    if not os.path.isdir(log_dir):
        warnings.warn("No Log Dir: '%s' found" % (log_dir), Warning)
        log_dir = '/tmp/'

    logger = logging.getLogger(name)

    base_name = os.path.basename(filename)
    log_formatter = MyFormatter(fmt='%%(asctime)s - "%s" - %%(levelname)s - %%(message)s' % (
        base_name,
        ))

    # -------------------------------------------------------------------------
    # Access Logging
    # -------------------------------------------------------------------------
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(os.path.join(log_dir, '%s.info.log' % (base_name)))
    fh.setFormatter(log_formatter)
    logger.addHandler(fh)

    # -------------------------------------------------------------------------
    # Error Logging
    # -------------------------------------------------------------------------
    efh = logging.FileHandler(os.path.join(log_dir, '%s.err.log' % (base_name)))
    efh.setFormatter(log_formatter)
    efh.setLevel(logging.ERROR)
    logger.addHandler(efh)

    return logger
