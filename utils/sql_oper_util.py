# coding:utf-8

import sys
import os
sys.path.append('/data/projects/job_manage')
from utils.mysql_util import MysqlUtil
from utils.file_util import get_complete_job
import os
import json
import time
import datetime

mu = MysqlUtil()

def get_job_status_from_sql(job_id):
    select_sql = "select f_status from fate_flow.t_job where f_job_id='%s'"%(job_id)
    flag, rows, data = mu.select(select_sql)
    try:
        return data[0][0]
    except:
        return 'mysql query error'

def get_job_id_from_sql():
    select_sql = 'select a.f_job_id, f_roles, a.f_party_id, f_table_name, f_table_namespace, f_component_name, a.f_create_time, f_task_id, a.f_description, f_dsl, f_runtime_conf, f_status, f_start_time, f_end_time from fate_flow.t_data_view a LEFT JOIN fate_flow.t_job b on a.f_job_id=b.f_job_id'
    if mu.conn != None:
        flag, rows, data = mu.select(select_sql)
    else:
        sql_res = os.popen("""docker exec confs-40001_mysql_1 sh -c 'mysql -uroot -Dfate_flow -e "%s"'""" % select_sql)
        sql_read = sql_res.readlines()
        data = [s_i.split('\t') for s_i in sql_read[1:]]
    
    job_complete_list = get_complete_job(method='r')
    
    job_id_list = list()
    data_add_list = list()
    for data_i in data:
        if len(job_id_list) > 10:
            break
        
        f_job_id, f_roles, f_party_id, f_table_name, f_table_namespace, f_component_name, f_create_time, f_task_id, f_description, f_dsl, f_runtime_conf, f_status, f_start_time, f_end_time = data_i
        
        if f_job_id in job_complete_list:
            continue
        if f_job_id in job_id_list:
            continue
        try:
            if f_end_time == 'null':
                run_time_second = (datetime.datetime.now() - f_start_time).seconds
            else:
                run_time_second = (f_end_time - f_start_time).seconds
        except:
            run_time_second = 0
        
        if 'secure_add_example_0' in f_runtime_conf or 'upload_' in f_runtime_conf:
            continue
        train_data = json.loads(f_runtime_conf).get('role_parameters').get('guest').get('args').get('data').get(
            'train_data')
        eval_data = json.loads(f_runtime_conf).get('role_parameters').get('guest').get('args').get('data').get(
            'eval_data')
        
        job_id_list.append(f_job_id)
        data_add_list.append((f_job_id, f_start_time, f_end_time, run_time_second, f_description, f_status,
                              json.loads(f_roles), train_data, eval_data))
        
        start_time = time.strftime("%Y%m%d", time.localtime(f_start_time / 1000))
        today = time.strftime("%Y%m%d", time.localtime(time.time()))
        if start_time < today:
            get_complete_job(method='a', job_id=f_job_id)
    
    return job_id_list, data_add_list