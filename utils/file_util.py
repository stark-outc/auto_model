# coding:utf-8
import sys
sys.path.append('/data/projects/job_manage')

# from config import COMPLETE_JOB, RUN_FAILED_FILE

def write_data(file_name, data, method='w', need_n=False):
    if need_n:
        data = data + '\n'
    with open(file_name, method) as f:
        f.write(data)
    f.close()

def read_data(file_name, method='r'):
    try:
        with open(file_name, method) as f:
            data = f.read()
        f.close()
    except:
        with open(file_name, 'rb') as f:
            data = f.read().decode('utf-8', 'ignore')
        f.close()
    return data

def get_complete_job(method='r', job_id=None):
    if method == 'r':
        try:
            with open(COMPLETE_JOB, 'r') as f:
                datas = f.read().split('\n')
            f.close()
            data_list = [data for data in datas if data]
        except:
            data_list = list()
        return data_list
    else:
        with open(COMPLETE_JOB, 'a') as f:
            if job_id:
                f.write(job_id + '\n')
        f.close()
        return [-1]
    

def failed_job_bak():
    data = read_data(RUN_FAILED_FILE)
    if data:
        write_data(RUN_FAILED_FILE+'.bak', '\n', 'a')
    write_data(RUN_FAILED_FILE+'.bak', data, 'a')
    write_data(RUN_FAILED_FILE, '')


if __name__ == '__main__':
    failed_job_bak()