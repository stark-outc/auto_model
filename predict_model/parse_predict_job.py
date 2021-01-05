import re
import os
import json
import sys
sys.path.append('/data/projects/job_manage')
from utils.log import logger
from utils.config import  ZK_DIR,HOST_TABLE_CONF
from utils.file_util import read_data,write_data

def get_host_data(proj_name):
    dir_list = os.listdir(ZK_DIR+proj_name)
    host_name_dict = dict()
    host_name_dict['host_table'] = {proj_name:{}}
    host_data = list()
    for i,j in enumerate(dir_list):
        if j[-1]=='g':
            month = sorted(re.findall(r'\d+',j))[0]
            log_data = read_data(ZK_DIR+proj_name+os.sep+j)
            finish_sign = re.findall('finish host feature request',log_data)
            try:
                if finish_sign:
                    host_name = re.findall('.*host_feature_table_name:(.*),host_feature_namespace',log_data)[0]
                    host_namespace = re.findall('.*host_feature_namespace:(.*)\n',log_data)[0]
                    host_data.append({month:{'name':host_name,'namespace':host_namespace}})
            except:
                logger.error(f'{month}_job is failed' )
    for i in host_data:
        host_name_dict['host_table'][proj_name].update(i)
    logger.info('get_host_table success')
    write_data(HOST_TABLE_CONF+f'{proj_name}_host_table.json',json.dumps(host_name_dict))
if __name__ == '__main__':
    proj_name = sys.argv[1]
    get_host_data(proj_name)
