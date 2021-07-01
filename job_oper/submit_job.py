import os
import sys
sys.path.append('/data/projects/fate/python/auto_model')
import json
import re
import ast
from utils.log import logger
from utils.file_util import read_data,write_data
from utils.config import HOST_TABLE_CONF,ROOT,ALL_TABLE,ZK_DIR,SUBMIT_MODEL,SUBMIT_JOB_PARAMS,N_TREES,LEARNING_RATE,TREE_DEEPTH,IV_VALUE_THRESHOLD,MIN_LEAF_NODE,MIN_SAMPLE_SPLIT,MAX_SPLIT_NODES,EARLY_STOPPING_ROUNDS,MAX_ITER
from utils.config import GUEST_ID,HOST_ID,LR_RUNTIME_CONFIG,LR_DSL,LR_RUNTIME_CONFIG_OLD,LR_DSL_OLD,XGB_RUNTIME_CONFIG,XGB_RUNTIME_CONFIG_OLD,XGB_DSL,XGB_DSL_OLD,VENV,FATE_FLOW_CILENT
from utils.get_model_param import board_status


def submit_job_xgb(p_json,datasource):
    num_trees = p_json.get('num_trees', N_TREES)
    learning_rate = p_json.get('learning_rate', LEARNING_RATE)
    max_trees = p_json.get('max_trees', TREE_DEEPTH)
    iv_value_threshold = p_json.get('iv_value_threshold', IV_VALUE_THRESHOLD)
    early_stopping_rounds = p_json.get('early_stopping_rounds',EARLY_STOPPING_ROUNDS)

    try:
        guest_table = p_json.get('guest_table', '')
        host_table = p_json.get('host_table', '')
    except:
        logger.error('guest_table or host_table is None')
        raise Exception

    guest_table_namespace = guest_table.get('namespace', 'hzph')
    guest_table_train = guest_table.get('train_table')
    guest_validate_train = guest_table.get('validate_table')
    host_table_namespace = host_table.get('namespace', 'host#10017#all#train_input')
    host_table_train = host_table.get('train_table')
    host_validate_train = host_table.get('validate_table')

    train_table = datasource.get('guest_table').get(guest_table_train)
    test_table = datasource.get('guest_table').get(guest_validate_train)
    # 判断是否使用旧版特征
    if p_json.get('features_type') == 'old':
        job_runtime_conf = read_data(XGB_RUNTIME_CONFIG_OLD)
        job_dsl = json.load(open(XGB_DSL_OLD, 'r'))

        # 修改dsl文件
        dsl_train_data = dict()
        dsl_test_data = dict()
        month_train = list(train_table.keys())
        month_test = list(test_table.keys())
        for o in range(len(month_train)):
            if o == 0:
                dsl_train_data['data'] = ['args.data_0']
            else:
                dsl_train_data[f'data_{o}'] = [f'args.data_{o}']
        for p, q in enumerate(range(len(month_train), len(month_train) + len(month_test))):
            if p == 0:
                dsl_test_data['data'] = [f'args.data_{q}']
            else:
                dsl_test_data[f'data_{p}'] = [f'args.data_{q}']
        job_dsl['components']['union_0']['input']['data'] = dsl_train_data
        job_dsl['components']['union_1']['input']['data'] = dsl_test_data
        write_data(XGB_DSL_OLD.replace('.bak',''), json.dumps(job_dsl))
        # 修改conf文件
        guest_name_train_table = list(train_table.values())
        guest_name_test_table = list(test_table.values())
        month_train.extend(month_test)
        host_table=dict()
        for i,j in enumerate(month_train):
            host_table[f"data_{i}"] = [datasource.get('host_table').get(host_table_train).get(j)]
        guest_name_train_table.extend(guest_name_test_table)
        guest_table = dict()
        for m,n in enumerate(guest_name_train_table):
            guest_table[f"data_{m}"]=[n]

        job_runtime_conf = job_runtime_conf.replace("train_test_guest_table", str(guest_table)).replace(
            "train_test_host_table", str(host_table)).replace("'", '"').replace('guest_id',str(GUEST_ID)).replace('host_id',str(HOST_ID))
        # 调整参数
        job_runtime_conf = job_runtime_conf.replace('iv_str', str(iv_value_threshold)).replace(
            'learning_rate_str',str(learning_rate)).replace('max_depth_str',str(max_trees)).replace(
            'early_stopping_rounds_str',str(early_stopping_rounds)).replace('num_trees_str',str(num_trees))
        write_data(XGB_RUNTIME_CONFIG_OLD.replace('.bak',''),job_runtime_conf)
        logger.info('guest_table%s \n host_table:%s' % (json.dumps(guest_table), json.dumps(host_table)))
        logger.info(
            f'num_trees {num_trees} learning_rate {learning_rate} max_trees {max_trees} iv {iv_value_threshold} num_trees {num_trees} early_stoppong_rounds {early_stopping_rounds}')
        job_res = os.popen(f'''source {VENV} && python {FATE_FLOW_CILENT} -f submit_job -d {XGB_DSL_OLD.replace('.bak','')} -c {XGB_RUNTIME_CONFIG_OLD.replace('.bak','')}''')
        job_read = job_res.readlines()
        logger.info('start job \n %s'%job_read)
        job_id_re = re.findall('"jobId": "(.*?)"', str(job_read))[0]
        logger.info('start job ,jobid is %s'%job_id_re)


    else:
        job_runtime_conf = read_data(XGB_RUNTIME_CONFIG)
        job_dsl = json.load(open(XGB_DSL, 'r'))
        # 修改dsl文件
        dsl_train_data = dict()
        dsl_test_data = dict()
        month_train = list(train_table.keys())
        month_test = list(test_table.keys())
        for o in range(len(month_train)):
            if o == 0:
                dsl_train_data['data'] = ['args.train_data_0']
            else:
                dsl_train_data[f'data_{o}'] = [f'args.train_data_{o}']
        for p in range(len(month_test)):
            if p == 0:
                dsl_test_data['data'] = [f'args.eval_data_0']
            else:
                dsl_test_data[f'data_{p}'] = [f'args.eval_data_{p}']
        job_dsl['components']['union_0']['input']['data'] = dsl_train_data
        job_dsl['components']['union_1']['input']['data'] = dsl_test_data
        write_data(XGB_DSL.replace('.bak',''), json.dumps(job_dsl))
        # 修改conf文件
        guest_name_train_table = list(train_table.values())
        guest_name_test_table = list(test_table.values())
        host_table = dict()

        for i,j in enumerate(month_train):
            host_table[f"train_data_{i}"] = [datasource.get('host_table').get(host_table_train).get(j)]

        for a,b in enumerate(month_test):
            host_table[f"eval_data_{a}"] = [datasource.get('host_table').get(host_table_train).get(b)]

        guest_table = dict()

        for m, n in enumerate(guest_name_train_table):
            guest_table[f"train_data_{m}"] = [n]

        for s, t in enumerate(guest_name_test_table):
            guest_table[f"eval_data_{s}"] = [t]

        job_runtime_conf = job_runtime_conf.replace("train_test_guest_table", str(guest_table)).replace(
            "train_test_host_table", str(host_table)).replace("'", '"').replace('guest_id',str(GUEST_ID)).replace('host_id',str(HOST_ID))
        # 调整参数
        job_runtime_conf = job_runtime_conf.replace('iv_str', str(iv_value_threshold)).replace(
            'learning_rate_str',str(learning_rate)).replace('max_depth_str',str(max_trees)).replace(
            'early_stopping_rounds_str',str(early_stopping_rounds)).replace('num_trees_str',str(num_trees))
        write_data(XGB_RUNTIME_CONFIG.replace('.bak',''),job_runtime_conf)
        logger.info('guest_table%s \n host_table:%s' % (json.dumps(guest_table), json.dumps(host_table)))
        logger.info(
            f'num_trees {num_trees} learning_rate {learning_rate} max_trees {max_trees} iv {iv_value_threshold} num_trees {num_trees} early_stoppong_rounds {early_stopping_rounds}')
        job_res = os.popen(f'''source {VENV} && python {FATE_FLOW_CILENT} -f submit_job -d {XGB_DSL.replace('.bak','')} -c {XGB_RUNTIME_CONFIG.replace('.bak','')}''')
        job_read = job_res.readlines()
        logger.info('start job \n %s'%job_read)
        job_id_re = re.findall('"jobId": "(.*?)"', str(job_read))[0]
        logger.info('start job ,jobid is %s'%job_id_re)




def submit_job_lr(p_json,datasource):
    num_trees = p_json.get('num_trees', N_TREES)
    learning_rate = p_json.get('learning_rate', LEARNING_RATE)
    max_trees = p_json.get('max_trees', TREE_DEEPTH)
    iv_value_threshold = p_json.get('iv_value_threshold', IV_VALUE_THRESHOLD)
    early_stopping_rounds = p_json.get('early_stopping_rounds', EARLY_STOPPING_ROUNDS)
    max_iter = p_json.get('max_iter', MAX_ITER)

    try:
        guest_table = p_json.get('guest_table', '')
        host_table = p_json.get('host_table', '')
    except:
        logger.error('guest_table or host_table is None')
        raise Exception

    guest_table_namespace = guest_table.get('namespace', 'hzph')
    guest_table_train = guest_table.get('train_table')
    guest_validate_train = guest_table.get('validate_table')
    host_table_namespace = host_table.get('namespace', 'host#10017#all#train_input')
    host_table_train = host_table.get('train_table')
    host_validate_train = host_table.get('validate_table')

    train_table = datasource.get('guest_table').get(guest_table_train)
    test_table = datasource.get('guest_table').get(guest_validate_train)
    # 判断是否使用旧版特征
    if p_json.get('features_type') == 'old':
        job_runtime_conf = read_data(LR_RUNTIME_CONFIG_OLD)
        job_dsl = json.load(open(LR_DSL_OLD, 'r'))

        # 修改dsl文件
        dsl_train_data = dict()
        dsl_test_data = dict()
        month_train = list(train_table.keys())
        month_test = list(test_table.keys())
        for o in range(len(month_train)):
            if o == 0:
                dsl_train_data['data'] = ['args.data_0']
            else:
                dsl_train_data[f'data_{o}'] = [f'args.data_{o}']
        for p, q in enumerate(range(len(month_train), len(month_train) + len(month_test))):
            if p == 0:
                dsl_test_data['data'] = [f'args.data_{q}']
            else:
                dsl_test_data[f'data_{p}'] = [f'args.data_{q}']
        job_dsl['components']['union_0']['input']['data'] = dsl_train_data
        job_dsl['components']['union_1']['input']['data'] = dsl_test_data
        write_data(LR_DSL_OLD.replace('.bak', ''), json.dumps(job_dsl))
        # 修改conf文件
        guest_name_train_table = list(train_table.values())
        guest_name_test_table = list(test_table.values())
        month_train.extend(month_test)
        host_table = dict()
        for i, j in enumerate(month_train):
            host_table[f"data_{i}"] = [datasource.get('host_table').get(host_table_train).get(j)]
        guest_name_train_table.extend(guest_name_test_table)
        guest_table = dict()
        for m, n in enumerate(guest_name_train_table):
            guest_table[f"data_{m}"] = [n]

        job_runtime_conf = job_runtime_conf.replace("train_test_guest_table", str(guest_table)).replace(
            "train_test_host_table", str(host_table)).replace("'", '"').replace('guest_id', str(GUEST_ID)).replace(
            'host_id', str(HOST_ID))
        # 调整参数
        job_runtime_conf = job_runtime_conf.replace('iv_str', str(iv_value_threshold)).replace(
            'learning_rate_str', str(learning_rate)).replace('max_depth_str', str(max_trees)).replace(
            'max_iter_str', str(max_iter)).replace('num_trees_str', str(num_trees)).replace('early_stopping_rounds_str',str(early_stopping_rounds))
        write_data(LR_RUNTIME_CONFIG_OLD.replace('.bak', ''), job_runtime_conf)
        logger.info('guest_table%s \n host_table:%s' % (json.dumps(guest_table), json.dumps(host_table)))
        logger.info(
            f'num_trees {num_trees} learning_rate {learning_rate} max_trees {max_trees} iv {iv_value_threshold} num_trees {num_trees} max_iter {max_iter}')
        job_res = os.popen(
            f'''source {VENV} && python {FATE_FLOW_CILENT} -f submit_job -d {LR_DSL_OLD.replace('.bak', '')} -c {LR_RUNTIME_CONFIG_OLD.replace('.bak', '')}''')
        job_read = job_res.readlines()
        logger.info('start job \n %s' % job_read)
        job_id_re = re.findall('"jobId": "(.*?)"', str(job_read))[0]
        logger.info('start job ,jobid is %s' % job_id_re)


    else:
        job_runtime_conf = read_data(LR_RUNTIME_CONFIG)
        job_dsl = json.load(open(LR_DSL, 'r'))
        # 修改dsl文件
        dsl_train_data = dict()
        dsl_test_data = dict()
        month_train = list(train_table.keys())
        month_test = list(test_table.keys())
        for o in range(len(month_train)):
            if o == 0:
                dsl_train_data['data'] = ['args.train_data_0']
            else:
                dsl_train_data[f'data_{o}'] = [f'args.train_data_{o}']
        for p in range(len(month_test)):
            if p == 0:
                dsl_test_data['data'] = [f'args.eval_data_0']
            else:
                dsl_test_data[f'data_{p}'] = [f'args.eval_data_{p}']
        job_dsl['components']['union_0']['input']['data'] = dsl_train_data
        job_dsl['components']['union_1']['input']['data'] = dsl_test_data
        write_data(LR_DSL.replace('.bak', ''), json.dumps(job_dsl))
        # 修改conf文件
        guest_name_train_table = list(train_table.values())
        guest_name_test_table = list(test_table.values())
        host_table = dict()

        for i, j in enumerate(month_train):
            host_table[f"train_data_{i}"] = [datasource.get('host_table').get(host_table_train).get(j)]

        for a, b in enumerate(month_test):
            host_table[f"eval_data_{a}"] = [datasource.get('host_table').get(host_table_train).get(b)]

        guest_table = dict()

        for m, n in enumerate(guest_name_train_table):
            guest_table[f"train_data_{m}"] = [n]

        for s, t in enumerate(guest_name_test_table):
            guest_table[f"eval_data_{s}"] = [t]

        job_runtime_conf = job_runtime_conf.replace("train_test_guest_table", str(guest_table)).replace(
            "train_test_host_table", str(host_table)).replace("'", '"').replace('guest_id', str(GUEST_ID)).replace(
            'host_id', str(HOST_ID))
        # 调整参数
        job_runtime_conf = job_runtime_conf.replace('iv_str', str(iv_value_threshold)).replace(
            'learning_rate_str', str(learning_rate)).replace('max_depth_str', str(max_trees)).replace(
            'max_iter_str', str(max_iter)).replace('num_trees_str', str(num_trees)).replace('early_stopping_rounds_str',str(early_stopping_rounds))
        write_data(LR_RUNTIME_CONFIG.replace('.bak', ''), job_runtime_conf)
        logger.info('guest_table%s \n host_table:%s' % (json.dumps(guest_table), json.dumps(host_table)))
        logger.info(
            f'num_trees {num_trees} learning_rate {learning_rate} max_trees {max_trees} iv {iv_value_threshold} num_trees {num_trees} max_iter {max_iter}')
        job_res = os.popen(
            f'''source {VENV} && python {FATE_FLOW_CILENT} -f submit_job -d {LR_DSL.replace('.bak', '')} -c {LR_RUNTIME_CONFIG.replace('.bak', '')}''')
        job_read = job_res.readlines()
        logger.info('start job \n %s' % job_read)
        job_id_re = re.findall('"jobId": "(.*?)"', str(job_read))[0]
        logger.info('start job ,jobid is %s' % job_id_re)

@board_status
def submit_job(p=None):
    # job_runtime_conf = read_data(ROOT + 'submit_model/' + 'job_runtime_conf_lr.json.bak')
    # job_dsl = json.load(open(ROOT + 'submit_model/' + 'job_dsl_lr.json.bak', 'r'))
    if p == None:
        params = read_data(SUBMIT_JOB_PARAMS).strip()
        if not params.strip():
            return -1
        p = params.split('\n')[0]
        params_rest = params[len(p) + 1:].strip()
    else:
        params = ''
        params_rest = ''
    logger.info('submit_job params is %s' % p)
    # 读入配置表
    with open(ZK_DIR+ALL_TABLE) as f:
        datasource = json.load(f)
    f.close()

    if p.strip():
        p_json = json.loads(p)
        # print (p_json)
    else:
        logger.info('no params data')
        return -1
    if p_json.get('method') == 'lr':
        job_id_re = submit_job_lr(p_json,datasource)
    else:
        job_id_re = submit_job_xgb(p_json,datasource)
    write_data(SUBMIT_JOB_PARAMS, params_rest)


if __name__ == '__main__':
    submit_job()

