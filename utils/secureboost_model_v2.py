# coding:utf-8

import json
import numpy as np
import sys
import os
sys.path.append('/data/projects/job_manage')
from config import MODEL_PATH

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(MyEncoder, self).default(obj)

def model_process(model_json_input,model_para_json,jobid):
    model_json = json.load(open(MODEL_PATH+jobid+os.sep+model_json_input))
    trees = model_json['data']["trees"]
    feature_map = model_json['data']['featureNameFidMapping']

    select_feature = set()

    for tree_id in range(len(trees)):
        tree = trees[tree_id]
        split_node_id = tree['splitMaskdict']
        tree_node_id = tree['tree']
        for self_tree_id in list(split_node_id.keys()):
            find_id = tree_node_id[int(self_tree_id)]
            print("The {:} tree use feature:".format(tree_id),feature_map[str(find_id['fid'])])
            select_feature.add(feature_map[str(find_id['fid'])])

    print("All used features:",sorted(list(select_feature)))
    model_para_json = json.load(open(MODEL_PATH+jobid+os.sep+model_para_json))
    learning_rate = model_para_json['data']['BoostingTreeParam']['learning_rate']
    use_missing = model_para_json['data']['BoostingTreeParam']['tree_param']['use_missing']
    zero_as_missing = model_para_json['data']['BoostingTreeParam']['tree_param']['zero_as_missing']

    model_json["learningRate"] = learning_rate
    model_json["useMissing"] = use_missing
    model_json["zeroAsMissing"] = zero_as_missing
    model_json["usedFeature"] = sorted(list(select_feature))


    with open(MODEL_PATH+jobid+os.sep + "sum_model_file_{:}.json".format(jobid), "w") as f:
        json.dump(model_json, f, cls=MyEncoder)


if __name__ == "__main__":

    model_json = sys.argv[1]
    model_para_json = sys.argv[2]
    jobid = sys.argv[3]

    model_process(model_json,model_para_json,jobid)


