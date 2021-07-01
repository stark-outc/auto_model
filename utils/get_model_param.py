# coding:utf-8

import sys

sys.path.append('..')
import re
import os
import time
import json
import datetime
from utils.job_outtime_util import check_job_run_too_long, get_debug_path, add_run_too_long_job
from utils.config import HOST,RUNNING_NUM
from utils.config import GUEST_ID, RUN_FAILED_FILE, SUBMIT_JOB_PARAMS, FAILED_JOB_RE_ADD, GUEST_ROLE, RUN_FAILED_JOB_TIME_LIMIT, RESTART_SH
from utils.http_util import post, get, put
from utils.file_util import write_data, read_data
from utils.log import logger


def update_desc(job_id, note):
    url_update_desc = 'http://%s:8080/job/update'%HOST
    data = '{"job_id":"%s","role":"guest","party_id":"%s","notes":"%s"}'%(job_id, GUEST_ID, note)
    put(url_update_desc, data=data)


def board_status(func):
    def wrapper(*args, **kwargs):
        url_status = 'http://%s:8080/job/query/status' % HOST
        r = get(url_status)
        if r.status_code != 200:
            raise ConnectionAbortedError
        r_json = r.json()
        running_num = len([i for i in r_json.get('data') if i.get('fRole') == 'guest' and i.get("fStatus") == "running"])
        waiting_num = len([i for i in r_json.get('data') if i.get('fRole') == 'guest' and i.get("fStatus") =="waiting"])
        to_deal_num = running_num+waiting_num
        if to_deal_num > RUNNING_NUM :
            logger.info('board running job is full')
            raise Exception
        else:
            logger.info(f'board has {to_deal_num} jobs ,now submit new job')
            res = func(*args,**kwargs)
            return res

    return wrapper

def get_fate_best_iteration(job_id='202007020659016481695', component_name=None):
    if component_name==None:
        component_name = 'secureboost_0'
    # url_best_iteration = 'http://%s:8080/v1/tracking/component/output/model' % HOST
    # data = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s"}'% (job_id, GUEST_ID, component_name)
    # r = post(url_best_iteration, json=json.loads(data))
    # if r.status_code != 200:
    #     raise ConnectionAbortedError
    # r_json = r.json()
    # try:
    #     best_iteration = r_json.get('data').get('data').get('bestIteration')
    # except:
    #     best_iteration = -1
    url_best_iteration = 'http://%s:8080/v1/tracking/component/metric_data' % HOST
    data = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"train","metric_name":"loss"}'% (job_id, GUEST_ID, component_name)
    r = post(url_best_iteration, data=data)
    if r.status_code != 200:
        raise ConnectionAbortedError
    r_json = r.json()
    try:
        loss_list = r_json.get('data').get('data')
        best_iteration = -1
        loss_ = 0
        loss_t = 0
        for index_loss, loss_i in enumerate(loss_list):
            abs_loss = abs(loss_ - loss_i[1])
            loss_ = loss_i[1]
            if  abs_loss < 0.002:
                loss_t += 1
                
                if loss_t >= 3:
                    best_iteration = index_loss + 1
                    break
                else:
                    continue
            else:
                loss_t = 0
        else:
            best_iteration  = len(loss_list)
        
    except:
        best_iteration = -1
    return best_iteration

def get_fate_iv_num(job_id='202007020659016481695', component_name='selection_0'):
    if component_name==None:
        component_name = 'selection_0'
    url_best_iteration = 'http://%s:8080/v1/tracking/component/output/model' % HOST
    data = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s"}'% (job_id, GUEST_ID, component_name)
    r = post(url_best_iteration, data=data)
    if r.status_code != 200:
        raise ConnectionAbortedError
    r_json = r.json()
    
    
    host_dict = dict()
    iv_list = [30, 50, 80, 100, 120, 150, 200]
    
    for i in iv_list:
        host_dict["iv_%s"%i] = -1
    try:
        featureValues = list(json.loads(re.findall('featureValues":({"host.*?})', r.text)[0]).values())

        featureValues_sorted = sorted([float(i) for i in featureValues], reverse=True)
        for i in iv_list:
            if i <= len(featureValues) -1:
                host_dict["iv_%s" % i] = featureValues_sorted[i-1]

    except:
        pass
        
    return host_dict


def query_status():
    url_status = 'http://%s:8080/job/query/status' % HOST

    r = get(url_status)
    if r.status_code != 200:
        raise ConnectionAbortedError
    r_json = r.json()

    running_num = len([i for i in r_json.get('data') if i.get('fRole') == 'guest' and i.get("fStatus") == "running"])
     
    return running_num


def get_waiting_job():
    url_status = 'http://%s:8080/job/query/status' % HOST

    r = get(url_status)
    if r.status_code != 200:
        raise ConnectionAbortedError
    r_json = r.json()

    waiting_job = [i.get('fJobId') for i in r_json.get('data') if i.get('fRole') == 'guest' and i.get("fStatus") =="waiting"]

    return waiting_job


def get_dsl_model(job_id='2020031018420484393472'):
    url_dsl = 'http://%s:8080/v1/pipeline/dag/dependencies' % HOST

    data = '{"job_id":"%s","role":"guest","party_id":"%s"}' % (job_id, GUEST_ID)
    r = post(url_dsl, data=data)
    if r.status_code != 200:
        raise ConnectionAbortedError
    r_json = r.json()

    component_need_run = r_json.get('data').get('component_need_run')
    dsl_model_list = [dsl_model for dsl_model in component_need_run.keys() if component_need_run[dsl_model] == True]

    return dsl_model_list


def get_params(job_id='2020031018420484393472', component_name='secureboost_0'):
    url_param = 'http://%s:8080/v1/tracking/component/parameters' % HOST
    data = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s"}' % (job_id, GUEST_ID, component_name)

    r = post(url_param, data=data)
    if r.status_code != 200:
        raise ConnectionAbortedError
    r_json = r.json()
    param_json = r_json.get('data')
    return param_json


def get_x_dims(job_id='2020031018420484393472'):
    url_param = 'http://%s:8080/v1/tracking/component/output/model' % HOST

    data = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"feature_selection_0"}' % (job_id, GUEST_ID)

    r = post(url_param, data=data)
    if r.status_code != 200:
        raise ConnectionAbortedError
    r_json = r.json()
    try:
        colNames = r_json.get('data').get('data').get('hostColNames')[0].get('colNames')
        col_valid = 0
        col_valid_num = -1

        for col_one in colNames:
            host_no = int(col_one.replace('host.', ''))
            if host_no > col_valid_num:
                col_valid += 1
                col_valid_num = host_no
            else:
                break
        x_dims = col_valid
    except:
        x_dims = -1
    return x_dims


def stop_job(job_id='2020031018420484393472'):
    logger.info("stop_job %s"%job_id)
    url_model_out_metric = 'http://%s:8080/job/v1/pipeline/job/stop' % HOST

    data_metric = '{"job_id":"%s","role":"guest","party_id":"%s"}' % (job_id, GUEST_ID)
    try:
        logger.d('url_model_out_metric is %s \ndata_metric is %s'%(url_model_out_metric, data_metric))
        r_metric = post(url_model_out_metric, data=data_metric)
        logger.d (r_metric.text)
        if "rpc request error" in r_metric.text:
            logger.error('rpc request error')
            os.system("sh %s"%RESTART_SH)
        time.sleep(20)
    except:
        logger.error('post url_model_out_metric occur error')
        os.system("sh %s"%RESTART_SH)
        time.sleep(20)
        
    # r_metric = post(url_model_out_metric, data=data_metric)


def re_add_job(job_id):
    logger.info('re_add_job job_id %s'%job_id)
    try:
        re_add_job_id = read_data(FAILED_JOB_RE_ADD)
    except:
        re_add_job_id = ''

    if job_id not in re_add_job_id:
        write_data(FAILED_JOB_RE_ADD,  '\n' + job_id , 'a')
        job_id_conf = get_job_id_conf(job_id)
        logger.info('job_id_conf %s' % job_id_conf)
        if job_id_conf == None:
            return

        json_data = job_id_conf.get('json_data')
        job_type = job_id_conf.get('job_type')
        try:
            job_create_time = job_id_conf.get('job_create_time')/1000
            is_too_old = datetime.datetime.strptime(RUN_FAILED_JOB_TIME_LIMIT, '%Y-%m-%d %H:%M:%S') > datetime.datetime.fromtimestamp(job_create_time)
        except:
            is_too_old = 1

        if job_type == 'train' and not is_too_old:
            logger.info('re_add_job job_id %s success' % job_id)
            write_data(SUBMIT_JOB_PARAMS, '\n' + json.dumps(json_data), 'a')
        else:
            logger.info('re_add_job job_id %s failed' % job_id)

def get_job_id_conf(job_id):
    url_job_id_conf = 'http://%s:8080/job/query/%s/%s/%s'%(HOST, job_id, GUEST_ROLE, GUEST_ID)
    r_conf = get(url_job_id_conf)
    if r_conf.json().get('msg') != 'OK':
        # res = {'train_ks': None, 'train_auc': None, 'validate_ks': None, 'validate_auc': None}
        return None
    else:
        job_create_time = r_conf.json().get('data').get('job').get('fCreateTime')
        job_id_conf_str = r_conf.json().get('data').get('job').get('fRuntimeConf')

        if 'secureboost_0' in job_id_conf_str and 'selection_0' in job_id_conf_str and 'evaluation_0' in job_id_conf_str:
            job_type = 'train'
        elif 'job_type' in job_id_conf_str and 'predict' in job_id_conf_str:
            job_type = 'predict'
            return None
        else:
            job_type = 'others'
            return None


        job_id_conf = json.loads(job_id_conf_str)

        iv_threshold = job_id_conf.get('algorithm_parameters').get('selection_0').get('iv_value_param').get('value_threshold')
        param_json_secureboost = job_id_conf.get('algorithm_parameters').get('secureboost_0')
        if param_json_secureboost:
            nun_trees = param_json_secureboost.get('num_trees')
            learning_rate = param_json_secureboost.get('learning_rate')
            tree_deepth = param_json_secureboost.get('tree_param').get('max_depth')
            min_sample_split = param_json_secureboost.get('tree_param').get('min_sample_split')
            min_leaf_node = param_json_secureboost.get('tree_param').get('min_leaf_node')
            max_split_nodes = param_json_secureboost.get('tree_param').get('max_split_nodes')
        else:
            nun_trees = None
            learning_rate = None
            tree_deepth = None
            min_sample_split = None
            min_leaf_node = None
            max_split_nodes = None


        guest_table_namespace = job_id_conf.get('role_parameters').get('guest').get('args').get('data').get('train_data')[0].get('namespace')
        guest_table_train = job_id_conf.get('role_parameters').get('guest').get('args').get('data').get('train_data')[0].get('name')
        guest_table_validate = job_id_conf.get('role_parameters').get('guest').get('args').get('data').get('eval_data')[0].get('name')
        guest_table = {'namespace': guest_table_namespace, 'train_table': guest_table_train, 'validate_table': guest_table_validate}

        host_table_id = job_id_conf.get('role').get('host')
        host_table_train = job_id_conf.get('role_parameters').get('host').get('args').get('data').get('train_data')
        host_table_validate = job_id_conf.get('role_parameters').get('host').get('args').get('data').get('eval_data')
        host_table = list()
        for index_i, host_table_id_i in enumerate(host_table_id):
            dict_host = dict()
            dict_host['host_id'] = host_table_id_i
            dict_host['namespace'] = host_table_train[index_i].get('namespace')
            dict_host['train_table'] = host_table_train[index_i].get('name')
            dict_host['validate_table'] = host_table_validate[index_i].get('name')
            host_table.append(dict_host)

        json_data = {"guest_table":guest_table, "host_table":host_table,
                     "n_trees": nun_trees, "learning_rate": learning_rate, "tree_deepth": tree_deepth,
                     "iv_value_threshold": iv_threshold,
                     "min_sample_split": min_sample_split, "min_leaf_node": min_leaf_node,
                     "max_split_nodes": max_split_nodes}

        return {'json_data':json_data, 'job_type':job_type, 'job_create_time':job_create_time}

def get_model_out(job_id='2020031018420484393472', component_name='evaluation_0'):
    url_model_out_metric = 'http://%s:8080/v1/tracking/component/metrics' % HOST
    url_model_out = 'http://%s:8080/v1/tracking/component/metric_data' % HOST

    data_metric = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s"}' % (
    job_id, GUEST_ID, component_name)
    # r_metric = post(url_model_out_metric, data=data_metric)
    # if r_metric.json().get('msg') != 'success':
    #     res = {'train_ks': None, 'train_auc': None, 'validate_ks': None, 'validate_auc': None}
    #
    # else:
    

    data_train = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"train","metric_name":"secureboost_0"}' % (
    job_id, GUEST_ID, component_name)
    data_validate = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"validate","metric_name":"secureboost_0"}' % (
    job_id, GUEST_ID, component_name)
    # metric_namespace = 'validate'
    # metric_namespace = 'train'
    r_train = post(url_model_out, data=data_train)
    r_validate = post(url_model_out, data=data_validate)
    if r_train.status_code != 200 or r_train.status_code != 200:
        raise ConnectionAbortedError
    r_train_json = r_train.json()
    train_model_list = r_train_json.get('data').get('data')
    train_model_json = dict()
    if train_model_list:
        for train_model_i in train_model_list:
            train_model_json[train_model_i[0]] = train_model_i[1]
    r_validate_json = r_validate.json()
    validate_model_list = r_validate_json.get('data').get('data')
    validate_model_json = dict()
    if validate_model_list:
        for validate_model_i in validate_model_list:
            validate_model_json[validate_model_i[0]] = validate_model_i[1]

    res = {'train_ks': train_model_json.get('ks'), 'train_auc': train_model_json.get('auc'),
           'validate_ks': validate_model_json.get('ks'), 'validate_auc': validate_model_json.get('auc'), }

    return res


def get_lr_model_out(job_id='2020031018420484393472', component_name='evaluation_0'):
    url_model_out_metric = 'http://%s:8080/v1/tracking/component/metrics' % HOST
    url_model_out = 'http://%s:8080/v1/tracking/component/metric_data' % HOST

    data_metric = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s"}' % (
        job_id, GUEST_ID, component_name)
    # r_metric = post(url_model_out_metric, data=data_metric)
    # if r_metric.json().get('msg') != 'success':
    #     res = {'train_ks': None, 'train_auc': None, 'validate_ks': None, 'validate_auc': None}
    #
    # else:

    data_train = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"train","metric_name":"hetero_lr_0"}' % (
        job_id, GUEST_ID, component_name)
    data_validate = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"validate","metric_name":"hetero_lr_0"}' % (
        job_id, GUEST_ID, component_name)
    # metric_namespace = 'validate'
    # metric_namespace = 'train'
    r_train = post(url_model_out, data=data_train)
    r_validate = post(url_model_out, data=data_validate)
    if r_train.status_code != 200 or r_train.status_code != 200:
        raise ConnectionAbortedError
    r_train_json = r_train.json()
    train_model_list = r_train_json.get('data').get('data')
    train_model_json = dict()
    if train_model_list:
        for train_model_i in train_model_list:
            train_model_json[train_model_i[0]] = train_model_i[1]
    r_validate_json = r_validate.json()
    validate_model_list = r_validate_json.get('data').get('data')
    validate_model_json = dict()
    if validate_model_list:
        for validate_model_i in validate_model_list:
            validate_model_json[validate_model_i[0]] = validate_model_i[1]

    res = {'train_ks': train_model_json.get('ks'), 'train_auc': train_model_json.get('auc'),
           'validate_ks': validate_model_json.get('ks'), 'validate_auc': validate_model_json.get('auc'), }

    return res


def get_model_ks_step(job_id='2020042307560864318746', component_name='secureboost_0'):
    url_model_out_metric = 'http://%s:8080/v1/tracking/component/metric_data' % HOST

    data_metric = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"train","metric_name":"loss"}' % (
        job_id, GUEST_ID, component_name)

    r_metric_loss = post(url_model_out_metric, data=data_metric)
    iter_ = len(r_metric_loss.json().get('data').get('data'))

    for i in range(iter_):
        try:
            iteration_ = 'iteration_%s' % i
            data_metric_train = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"train","metric_name":"%s"}' % (
            job_id, GUEST_ID, component_name, iteration_)
            data_metric_eval = '{"job_id":"%s","role":"guest","party_id":"%s","component_name":"%s","metric_namespace":"validate","metric_name":"%s"}' % (
            job_id, GUEST_ID, component_name, iteration_)
            r_metric_train = post(url_model_out_metric, data=data_metric_train)
            r_metric_validate = post(url_model_out_metric, data=data_metric_eval)

            r_metric_data_train = r_metric_train.json().get('data').get('data')
            r_metric_data_dict = dict()
            r_metric_data_dict['train_' + r_metric_data_train[0][0]] = r_metric_data_train[0][1]
            r_metric_data_dict['train_' + r_metric_data_train[1][0]] = r_metric_data_train[1][1]

            r_metric_data_validate = r_metric_validate.json().get('data').get('data')
            r_metric_data_dict['validate_' + r_metric_data_validate[0][0]] = r_metric_data_validate[0][1]
            r_metric_data_dict['validate_' + r_metric_data_validate[1][0]] = r_metric_data_validate[1][1]

            train_auc = r_metric_data_dict.get('train_auc')
            train_ks = r_metric_data_dict.get('train_ks')
            validate_auc = r_metric_data_dict.get('validate_auc')
            validate_ks = r_metric_data_dict.get('validate_ks')

            print (iteration_, 'train_auc %s' % train_auc, 'validate_auc %s' % validate_auc, 'train_ks %s' % train_ks,
                  'validate_ks %s' % validate_ks)

        except:
            break

        a = 1

    return





if __name__ == '__main__':
    # job_id_list, data_add_list = get_job_id()
    # print (job_id_list, data_add_list)
    #
    # dsl_model_list = get_dsl_model()
    #
    # param_json = get_params()

    # res = get_model_out()
    # stop_job('2020080105102514887738')
    # p = get_params('202004271032187454208')
    # p_1 = get_params('202004271032187454208', 'selection_0')

    # re_add_job('202004301639591885868')
    # get_job_id_conf('2020051202340634289520')
    # print (p)

    # get_model_ks_step()
    # get_fate_best_iteration("2020071502495300804020")
    # get_fate_iv_num()
    #print (get_waiting_job())
    # get_lr_model_out(job_id='2020122416210371398093', component_name='evaluation_0')
    # a = 1
    print(query_status())