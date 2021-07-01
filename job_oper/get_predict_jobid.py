# coding:utf-8


import json
import sys
import os
sys.path.append('..')
from utils.mysql_util import MysqlUtil
from utils.log import logger
import json
from utils.config import ZK_DIR,ALL_TABLE
mu = MysqlUtil()


def insert_predict_jobid_data(data_add):
    insert_sql = '''insert into predict_jobid(create_date,job_id ,model_job_id,data_source,predict_model_type,predict_month) values('%s','%s','%s', '%s', '%s', '%s')''' % tuple(
        data_add)
    mu.insert(insert_sql.replace('"None"', 'null').replace("'None'", 'null'))


def parse_predict_jobid_func():
    logger.info('parse_predict_job')
    success_job_id_sql = '''
        select f_job_id from t_job where f_tag="job_end" and f_status="success"
    '''

    flag, rows, success_job_id = mu.select(success_job_id_sql)
    success_job_id_list = [i[0] for i in success_job_id]
    logger.info('success_job_id_list len is %s' % (len(success_job_id_list)))

    param_saved_id = '''
            select job_id from predict_jobid
        '''
    flag, rows, param_saved_id = mu.select(param_saved_id)
    param_saved_id_list = [i[0] for i in param_saved_id]
    logger.info('param_saved_id_list len is %s' % (len(param_saved_id_list)))

    need_parse_id = list()
    for success_job_id_i in success_job_id_list:
        if success_job_id_i not in param_saved_id_list:
            need_parse_id.append(success_job_id_i)
    need_parse_id = set(need_parse_id)
    logger.info('need_parse_id len is %s' % (len(need_parse_id)))

    need_parse_id_str = "','".join(need_parse_id)
    conf_id = '''
        select f_create_date,f_job_id, f_runtime_conf from t_job where f_job_id in ('%s')
    ''' % need_parse_id_str
    flag, rows, need_parse_id_list = mu.select(conf_id)
    _all_table = json.load(open(ZK_DIR+ALL_TABLE,'r'))
    for need_parse_id_i in need_parse_id_list:
        logger.info('parse job id %s' % (need_parse_id_i[0]))
        f_create_date,f_job_id, f_runtime_conf = need_parse_id_i
        job_runtime_conf = json.loads(f_runtime_conf)
        if job_runtime_conf.get('job_parameters').get('job_type')!='predict':
            continue
        try:

            guest_data_comp = json.loads(f_runtime_conf).get('role_parameters').get('guest').get('args').get('data').get('data_0')
            host_data_comp = json.loads(f_runtime_conf).get('role_parameters').get('host').get('args').get('data').get('data_0')
            if guest_data_comp is None or host_data_comp is None:
                guest_data_comp = json.loads(f_runtime_conf).get('role_parameters').get('guest').get('args').get(
                    'data').get('train_data_0')
                host_data_comp = json.loads(f_runtime_conf).get('role_parameters').get('host').get('args').get(
                    'data').get('train_data_0')

            all_host_table = _all_table.get('host_table')
            month_lst=[]
            for k,v in all_host_table.items():
                t = [k1 for k1,v1 in v.items() if v1 == host_data_comp[0]]
                month_lst.extend(t)
            predict_month = month_lst[0]
            guest_data_comp.extend(host_data_comp)
            data_list = [data_i.get('name') for data_i in guest_data_comp]
            data_source = '#'.join(data_list)
        except:
            logger.error('parse predict_data error job id %s' % (need_parse_id_i[0]))
            predict_month = None
            data_source = None
        try:
            model_jobid = json.loads(f_runtime_conf).get('job_parameters').get('model_version')
        except:
            logger.error('parse model_jobid error job id %s' % (need_parse_id_i[0]))
            model_jobid = None
        lr_model_sign = json.loads(f_runtime_conf).get('role').get('arbiter')
        if lr_model_sign==None:
            predict_model_type='secureboost_0'
        else:
            predict_model_type='hetero_lr_0'

        create_date = f_create_date
        job_id = f_job_id
        insert_predict_jobid_data(
            [create_date,job_id, model_jobid,data_source,predict_model_type,predict_month])


if __name__ == '__main__':
    parse_predict_jobid_func()

