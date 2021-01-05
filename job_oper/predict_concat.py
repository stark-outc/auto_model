import os
import sys
sys.path.append('/data/projects/fate/python/auto_model')
import ast
from utils.config import DOWNLOAD_DIR,PREDICT_DIR
from utils.file_util import read_data
import pandas as pd
def predict_concat(guest_name,dir_name,save_name,model_type=None):
    root = DOWNLOAD_DIR+f'{dir_name}_model_out'
    save_file_name = save_name+'.csv'
    dir_lst = os.listdir(root)
    # 读取本次预测的jobid
    try:
        tmp = read_data(PREDICT_DIR+guest_name+'_'+'predict_jobid.txt')
        lst = ast.literal_eval(tmp)
    except:
        logger.error(f"{guest_name}_predict_jobid.txt is not exists")
    jobid_lst=list()
    for lst_i in lst:
        download_jobid = list(lst_i.values())[0]
        jobid_lst.append(download_jobid)
    data_lst = []
    for dir_i in dir_lst:
        for dir_j in os.listdir(root + os.sep + dir_i):
            if model_type:
                if dir_j in [f'job_{jobid_i}_hetero_lr_0_guest_71000_output_data' for jobid_i in jobid_lst]:
                    data_name = root + os.sep + dir_i + os.sep + dir_j + '/output_data.csv'
                    print(data_name)
                    data = pd.read_csv(data_name, engine='python')
                    data['type'] = dir_i[:-7]
                    data['month'] = dir_i[-6:]
                    data_lst.append(data)
                    data_all = pd.concat(data_lst, axis=0)

                    data_all.to_csv(DOWNLOAD_DIR  + save_file_name)
            else:
                if dir_j in [f'job_{jobid_i}_secureboost_0_guest_71000_output_data' for jobid_i in jobid_lst]:
                    data_name = root + os.sep + dir_i + os.sep + dir_j + '/train.csv'
                    print(data_name)
                    data = pd.read_csv(data_name, engine='python')
                    data['type'] = dir_i[:-7]
                    data['month'] = dir_i[-6:]
                    data_lst.append(data)
                    data_all = pd.concat(data_lst, axis=0)

                    data_all.to_csv(DOWNLOAD_DIR + save_file_name)
if __name__ == '__main__':
    guest_name = sys.argv[1]
    dir_name = sys.argv[2]
    save_name = sys.argv[3]
    model_type = sys.argv[4]
    predict_concat(guest_name,dir_name,save_name,model_type)
