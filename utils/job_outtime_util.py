# coding:utf-8

import sys
import os
sys.path.append('/data/projects/job_manage')
import datetime
import re
from utils.config import DEBUG_LOG,  RUN_TOO_LONG_FILE,  DEBUG_LOG_OT
from utils.log import logger


def get_debug_path(job_id):
    for ROLE in ['host', 'guest']:
        debug_log_path = DEBUG_LOG%(job_id, ROLE)
        if os.path.exists(debug_log_path):
            break
    return debug_log_path


def check_job_run_too_long(debug_log_path):
    logger.debug(debug_log_path)
    try:
        with open(debug_log_path, 'r') as f:
            debug_logs = f.read()
        f.close()
    except:
        logger.error('debug_log_path is incorrect \n%s'%debug_log_path)
        return 1
    #log_recent = debug_logs.strip().split('\n')[-1].split(',')[0].replace('"', '')
    log_recent = re.findall("\[DEBUG\] \[(\d*-\d*-\d* \d*:\d*:\d*),\d*\]", debug_logs.strip(), re.S)
    if not log_recent:
        logger.error('debug_log data is invalid')
        return 0
    log_recent = log_recent[-1]
    logger.debug('log_recent is %s'%log_recent)
    dt_log_recent = datetime.datetime.strptime(log_recent, '%Y-%m-%d %H:%M:%S')
    dt_now = datetime.datetime.now()
    dt_detal = dt_now - dt_log_recent
    dt_detal_hours = dt_detal.seconds/3600+float(dt_detal.days*24)
    debug_ot = 8 + DEBUG_LOG_OT
    logger.debug('dt_log_recent %s'%dt_log_recent)
    logger.debug('dt_now %s'%dt_now)

    if dt_detal_hours > debug_ot:
        logger.info('debug log stop too log, time is %s' % (debug_ot - dt_detal_hours))
        return 1

    else:
        # logger.info('job runs normal')
        return 0

def add_run_too_long_job(job_id):
    logger.info('add_run_too_long_job job id is %s'%job_id)
    with open(RUN_TOO_LONG_FILE, 'r') as f1:
        data = f1.read()
    f1.close()
    if job_id not in data:
        logger.info('add_run_too_long_job job success' )
        with open(RUN_TOO_LONG_FILE, 'a') as f:
            f.write(job_id+'\n')
        f.close()
    else:
        logger.info('add_run_too_long_job job failed, job_id exists' )



if __name__ == '__main__':
    job_id = sys.argv[1]
    print (job_id)
    if check_job_run_too_long(get_debug_path(job_id)):
        add_run_too_long_job(job_id)
    # check_job_run_too_long('DEBUG.log')
