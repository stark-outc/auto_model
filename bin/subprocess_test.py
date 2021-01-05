import subprocess
ret = subprocess.Popen(["sh","secureboost_param_tosql.sh"],shell=False,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
stdout,stderr=ret.communicate()
stdout = stdout.decode('utf-8')
print(stdout)
# import os
# log_pre = os.popen("sh secureboost_param_tosql.sh")
# log_pre = log_pre.readlines()
# print(log_pre)