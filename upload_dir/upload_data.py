import os
import json
import time
import sys
sys.path.append('/data/projects/fate/python/auto_model')
import re
from utils.log import logger
from utils.file_util import read_data
from utils.config import ZK_DIR
def zk_upload(proj_name):
    lists = os.listdir(ZK_DIR+str(proj_name))
    log_list = [i[:6] for i in lists if i[-1]=='g']
    csv_list = [i[:6] for i in lists if i[-1]=='v']
    for i in log_list:
        log_data = read_data(ZK_DIR+str(proj_name)+os.sep+i+'.log')
        failed_job = re.findall('Traceback',log_data)
        if failed_job:
            logger.error(f'{i}_job is failed')
    to_upload =sorted([i for i in csv_list if i not in log_list])
    if len(to_upload)>0:
        upload_month = to_upload[0]
        upload_data_file = ZK_DIR+str(proj_name)+os.sep+str(upload_month)+'.csv'
        with open('/data/projects/fate/python/auto_model/config/setting.json','rb') as f:
            data = json.load(f)
        f.close()
        try:
            data['upload_data_file']=upload_data_file
            logger.info('rewrite setting.json success')
            if str(upload_month)[-2::]=='01':
                data['stat_month']=str(int(str(upload_month)[:4])-1)+'12'
            else:
                data['stat_month']=str(int(upload_month)-1)
            json_data = json.dumps(data)
        except:
            logger.error(f'{upload_month} rewrite setting.josn failed')
        with open('/data/projects/fate/python/auto_model/config/setting.json','w') as fw:
            fw.write(json_data)
        fw.close()
        os.system("""source /data/projects/fate/bin/init_env.sh && python /data/projects/fate/python/auto_model/run_task.py -f upload -r guest -t intersect""")
        time.sleep(20)
        os.system("""source /data/projects/fate/bin/init_env.sh && cd /data/projects/fate/python/auto_model && nohup python -u run_task.py -f intersect > /data/projects/fate/python/auto_model/upload_dir/%s/%s.log &"""%(proj_name,str(upload_month)))

if __name__ == '__main__':
    proj_dir = sys.argv[1]
    zk_upload(proj_dir)



