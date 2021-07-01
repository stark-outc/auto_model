# coding:utf-8


import json
import sys

sys.path.append('..')
from utils.mysql_util import MysqlUtil
from utils.get_model_param import get_lr_model_out, get_x_dims
from utils.log import logger

mu = MysqlUtil()


def insert_lr_param_data(data_add):
    insert_sql = '''insert into lr_param(create_date,job_id ,data_source,component_name, x_dims, iv_min, penalty,alpha,max_iter, early_stopping_rounds, sqn_param_update_interval_L,sqn_param_update_memory_M, sqn_param_update_sample_size,learning_rate, train_ks, validate_ks, train_auc, validate_auc) values('%s', '%s','%s', '%s','%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s','%s', '%s','%s', '%s','%s')''' % tuple(
        data_add)
    mu.insert(insert_sql.replace('"None"', 'null').replace("'None'", 'null'))


def parse_lr_param_func():
    logger.info('parse_lr_param')
    success_job_id_sql = '''
        select f_job_id from t_job where f_tag="job_end" and f_status="success"
    '''

    flag, rows, success_job_id = mu.select(success_job_id_sql)
    success_job_id_list = [i[0] for i in success_job_id]
    logger.info('success_job_id_list len is %s' % (len(success_job_id_list)))

    param_saved_id = '''
            select job_id from lr_param
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

    for need_parse_id_i in need_parse_id_list:
        logger.info('parse job id %s' % (need_parse_id_i[0]))
        f_create_date,f_job_id, f_runtime_conf = need_parse_id_i
        if 'hetero_lr_' not in f_runtime_conf:
            continue
        try:
            train_data_comp = json.loads(f_runtime_conf).get('role_parameters').get('guest').get('args').get('data')
            train_data_list = sorted(list(set([train_data_i[0].get('name') for train_data_i in train_data_comp.values()])))
            train_data = '#'.join(train_data_list)
        except:
            logger.error('parse train_data error job id %s' % (need_parse_id_i[0]))
            train_data = None
        try:
            iv_min = json.loads(f_runtime_conf).get('role_parameters').get('host').get(
                'hetero_feature_selection_0').get('iv_value_param').get('value_threshold')[0]
        except:
            logger.error('parse iv_min error job id %s' % (need_parse_id_i[0]))
            iv_min = None
        if json.loads(f_runtime_conf).get('algorithm_parameters') == None:
            logger.error('parse algorithm_parameters error job id %s' % (need_parse_id_i[0]))
            continue
        lr_Param = json.loads(f_runtime_conf).get('algorithm_parameters').get('hetero_lr_0')
        if lr_Param == None:
            logger.error('parse hetero_lr_0 error job id %s' % (need_parse_id_i[0]))
            continue
        create_date = f_create_date
        job_id = f_job_id

        component_name = 'evaluation_0'

        x_dims = get_x_dims(job_id=f_job_id)

        ks_auc_dict = get_lr_model_out(job_id=f_job_id)
        train_ks = ks_auc_dict.get('train_ks', 0)
        validate_ks = ks_auc_dict.get('validate_ks', 0)
        train_auc = ks_auc_dict.get('train_auc', 0)
        validate_auc = ks_auc_dict.get('validate_auc', 0)
        try:
            early_stopping_rounds = lr_Param.get('early_stopping_rounds')
            sqn_param_update_interval_L = lr_Param.get('sqn_param').get('update_interval_L')
            sqn_param_update_memory_M = lr_Param.get('sqn_param').get('memory_M')
            sqn_param_update_sample_size = lr_Param.get('sqn_param').get('sample_size')
        except :
            early_stopping_rounds = None
            sqn_param_update_interval_L = None
            sqn_param_update_memory_M = None
            sqn_param_update_sample_size = None
        try:
            penalty = lr_Param.get('penalty')
            alpha = lr_Param.get('alpha')
            max_iter = lr_Param.get('max_iter')
            # early_stopping_rounds = lr_Param.get('early_stopping_rounds')
            learning_rate = lr_Param.get('learning_rate')
            # sqn_param_update_interval_L = lr_Param.get('sqn_param').get('update_interval_L')
            # sqn_param_update_memory_M = lr_Param.get('sqn_param').get('memory_M')
            # sqn_param_update_sample_size = lr_Param.get('sqn_param').get('sample_size')
        except:
            penalty = None
            alpha = None
            max_iter = None
            # early_stopping_rounds = None
            # sqn_param_update_interval_L = None
            # sqn_param_update_memory_M = None
            learning_rate = None
            # sqn_param_update_sample_size = None

        insert_lr_param_data(
            [create_date,job_id, train_data, component_name, x_dims, iv_min, penalty,
             alpha,
             max_iter, early_stopping_rounds, sqn_param_update_interval_L,
             sqn_param_update_memory_M, sqn_param_update_sample_size,learning_rate, train_ks, validate_ks, train_auc, validate_auc])


if __name__ == '__main__':
    parse_lr_param_func()

