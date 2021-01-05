# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 18:09:45 2020

@author: 徐钦华
"""
import os
import json
import sys

def predict_func(month,model_id,guest_name,proj_name):
    # host_table = pd.read_json('host_month.json')
    with open('/data/projects/fate/python/auto_model/predict_model/host_month.json','rb') as f:
        host_table = json.load(f)
    f.close()
    if proj_name=='jb':
        host_name = host_table.get('host_data_jb').get(str(month))[0]
    else:
        host_name = host_table.get('host_data').get(str(month))[0]
    
    with open('/data/projects/fate/python/auto_model/predict_model/predict.json','rb') as fs:
        predict = json.load(fs)
    fs.close()
    predict['job_parameters']['model_version']=str(model_id)
    predict['role_parameters']['host']['args']['data']['data_0']= [host_name]
    predict['role_parameters']['guest']['args']['data']['data_0'][0]['name'] = guest_name
    json_data=json.dumps(predict)
    with open('/data/projects/fate/python/auto_model/predict_model/predict.json','w') as fw:
        fw.write(json_data)
    fw.close()
    os.system("""source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/auto_model/predict_model/ && sh s_predict.sh""")
if __name__=='__main__':
    month = sys.argv[1]
    model_id = sys.argv[2]
    guest_name = sys.argv[3]
    proj_name = sys.argv[4]
    predict_func(str(month),model_id,guest_name,proj_name)