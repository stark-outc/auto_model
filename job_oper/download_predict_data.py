import os
import json
import sys
sys.path.append('..')
import time
from utils.mysql_util import MysqlUtil
import re
from utils.log import logger
from utils.config import ZK_DIR,PREDICT_DIR,HOST_TABLE_CONF,DOWNLOAD_DIR
import pandas as pd
from utils.file_util import read_data,write_data
import ast

def download_data(guest_name,model_id,save_name):
    # output_data
    mu = MysqlUtil()
    os.system("""source /data/projects/fate/bin/init_env.sh && python /data/projects/fate/python/auto_model/job_oper/get_predict_jobid.py""")
    parse_jobid_sql = f"""SELECT max(a.create_date) AS create_date,max(a.job_id) AS job_id,a.model_job_id,a.data_source,a.predict_model_type,a.predict_month from (SELECT * FROM predict_jobid  WHERE model_job_id='{model_id}' and data_source like '{guest_name}%' ) as a GROUP BY model_job_id,predict_model_type,predict_month"""
    flag, rows, predict_saved_id = mu.select(parse_jobid_sql)
    job_id_lst = []
    for parse_jobid in predict_saved_id:
        create_date,job_id, model_job_id, data_source, predict_model_type, predict_month = parse_jobid
        # print(job_id,model_job_id,data_source,predict_model_type,predict_month)
        logger.info(f'{guest_name} predict_month is {predict_month} \nand model_type is {predict_model_type} \nand job_id is{job_id}')
        try:
            os.system(
                """source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/fate_flow/ && python fate_flow_client.py -f component_output_data -j {} -r guest -p 71000 -cpn {} -o /data/projects/fate/python/auto_model/moder_out/{}/{}_{}""".format(
                    str(job_id), predict_model_type, model_id, guest_name, str(predict_month)))
        except:
            logger.error(f"job_id:{job_id}download failed")
        job_id_lst.append(job_id)
    print(job_id_lst)
    #concat data
    root = DOWNLOAD_DIR + f'{model_id}'
    save_file_name = save_name + '.csv'
    dir_lst = os.listdir(root)
    data_lst=[]
    for dir_i in dir_lst:
        if guest_name in dir_i:
            for dir_j in os.listdir(root + os.sep + dir_i):
                print(dir_j)
                job_id_this = [str(m) for m in job_id_lst if m in dir_j]
                if len(job_id_this)>0 and job_id_this[0] in dir_j:
                    if 'hetero_lr_0' in dir_j:
                        data_name = root + os.sep + dir_i + os.sep + dir_j + os.sep +'output_data.csv'
                        if not os.path.exists(data_name):
                            data_name = root + os.sep + dir_i + os.sep + dir_j + os.sep + 'train.csv'
                    else:
                        data_name = root + os.sep + dir_i + os.sep + dir_j + os.sep + 'train.csv'
                    logger.info(f'concat data_name is {data_name}')


                
                    data = pd.read_csv(data_name, engine='python')
                    data['type'] = dir_i[:-7]
                    data['month'] = dir_i[-6:]
                    data_lst.append(data)
    data_all = pd.concat(data_lst, axis=0)
    data_all.to_csv(DOWNLOAD_DIR  + save_file_name,mode='w')

if __name__ == '__main__':
    guest_name = sys.argv[1]
    model_id = sys.argv[2]
    save_name = sys.argv[3]
    # guest_name = 'sj_all'
    # model_id = '202104101554210708684008'
    # save_name = '202104101554210708684008_sj_all'
    download_data(guest_name,model_id,save_name)

    # download_data('sn_0207',202012281628089492731,'ms_sn_0207.csv')