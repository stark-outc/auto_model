# coding:utf-8
import sys
sys.path.append('/data/projects/job_manage')

import logging
import logging.handlers
import datetime
from utils.config import ALL_LOG, ERR_LOG


class Logger():
    def __init__(self):
        self.logger = logging.getLogger('job_manage')
        self.logger.setLevel(logging.DEBUG)
    
        rf_handler = logging.handlers.TimedRotatingFileHandler(ALL_LOG, when='midnight', interval=1, backupCount=7,
                                                               atTime=datetime.time(0, 0, 0, 0))
        rf_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
        f_handler = logging.FileHandler(ERR_LOG)
        ch = logging.StreamHandler()
    
        f_handler.setLevel(eval("logging.%s" % 'ERROR'))
        f_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
    
        ch.setLevel(eval("logging.%s" % 'INFO'))
        ch.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
    
        self.logger.addHandler(rf_handler)
        self.logger.addHandler(f_handler)
        self.logger.addHandler(ch)
        
        # logger.debug('debug message')
        # logger.info('info message')
        # logger.warning('warning message')
        # logger.error('error message')
        # logger.critical('critical message')
    
        def i(self, msg):
            return self.logger.info(msg)


logger = Logger().logger