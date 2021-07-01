# coding:utf-8


import json
import sys

sys.path.append('..')
from utils.mysql_util import MysqlUtil
from utils.get_model_param import get_model_out, get_x_dims
from utils.log import logger

mu = MysqlUtil()


def insert_secureboost_param_data(data_add):
    insert_sql = '''insert into secureboost_param(create_date, job_id ,data_source,component_name, x_dims, bin_num, iv_min, tree_param_max_depth, tree_param_max_split_nodes, tree_param_min_sample_split, tree_param_min_leaf_node, num_trees,subsample_feature_rate,learning_rate, train_ks, validate_ks, train_auc, validate_auc) values('%s','%s','%s', '%s','%s','%s', '%s', '%s','%s', '%s', '%s','%s', '%s','%s', '%s','%s', '%s','%s')''' % tuple(
        data_add)
    mu.insert(insert_sql.replace('"None"', 'null').replace("'None'", 'null'))


def parse_secureboost_param_func():
    logger.info('parse_secureboost_param')
    success_job_id_sql = '''
        select f_job_id from t_job where f_tag="job_end" and f_status="success"
    '''

    flag, rows, success_job_id = mu.select(success_job_id_sql)
    success_job_id_list = [i[0] for i in success_job_id]
    logger.info('success_job_id_list len is %s' % (len(success_job_id_list)))

    param_saved_id = '''
            select job_id from secureboost_param
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
        if 'secureboost_' not in f_runtime_conf:
            continue
        try:
            train_data_comp = json.loads(f_runtime_conf).get('role_parameters').get('guest').get('args').get('data')
            train_data_list = sorted(list(set([train_data_i[0].get('name') for train_data_i in train_data_comp.values()])))
            train_data = '#'.join(train_data_list)
        except:
            logger.error('parse train_data error job id %s' % (need_parse_id_i[0]))
            train_data = None
        try:
            bin_num = json.loads(f_runtime_conf).get('algorithm_parameters').get('secureboost_0').get('bin_num')
        except:
            logger.error('parse bin_num error job id %s' % (need_parse_id_i[0]))
            bin_num = None
        try:
            iv_min = json.loads(f_runtime_conf).get('role_parameters').get('host').get(
                'hetero_feature_selection_0').get('iv_value_param').get('value_threshold')[0]
        except:
            logger.error('parse iv_min error job id %s' % (need_parse_id_i[0]))
            iv_min = None

        BoostingTreeParam = json.loads(f_runtime_conf).get('algorithm_parameters').get('secureboost_0')
        if BoostingTreeParam == None:
            logger.error('parse secureboost_0 error job id %s' % (need_parse_id_i[0]))
            continue

        job_id = f_job_id
        create_date = f_create_date
        component_name = 'evaluation_0'

        x_dims = get_x_dims(job_id=f_job_id)

        ks_auc_dict = get_model_out(job_id=f_job_id)
        train_ks = ks_auc_dict.get('train_ks', 0)
        validate_ks = ks_auc_dict.get('validate_ks', 0)
        train_auc = ks_auc_dict.get('train_auc', 0)
        validate_auc = ks_auc_dict.get('validate_auc', 0)
        try:
            tree_param_max_depth = BoostingTreeParam.get('tree_param').get('max_depth')
            tree_param_max_split_nodes = BoostingTreeParam.get('tree_param').get('max_split_nodes')
            tree_param_min_sample_split = BoostingTreeParam.get('tree_param').get('min_sample_split')
            tree_param_min_leaf_node = BoostingTreeParam.get('tree_param').get('min_leaf_node')
            num_trees = BoostingTreeParam.get('num_trees')
            subsample_feature_rate = BoostingTreeParam.get('subsample_feature_rate')
            learning_rate = BoostingTreeParam.get('learning_rate')
        except:
            tree_param_max_depth = None
            tree_param_max_split_nodes = None
            tree_param_min_sample_split = None
            tree_param_min_leaf_node = None
            num_trees = None
            subsample_feature_rate = None
            learning_rate = None

        insert_secureboost_param_data(
            [create_date, job_id, train_data, component_name, x_dims, bin_num, iv_min, tree_param_max_depth,
             tree_param_max_split_nodes,
             tree_param_min_sample_split, tree_param_min_leaf_node, num_trees,
             subsample_feature_rate, learning_rate, train_ks, validate_ks, train_auc, validate_auc])


if __name__ == '__main__':
    parse_secureboost_param_func()

