#
# main.py
# Francois Maillet, 2015-03-26
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import csv, datetime

mldb.log("Pytanic Plugin Executing setup...")


print mldb.perform("DELETE", "/v1/datasets/titanic_train", [], {})
print mldb.perform("DELETE", "/v1/datasets/titanic_test", [], {})

# load the train and test datasets
for dataset_type in ["train", "test"]:
    datasetConfig = {
            "type": "sparse.mutable",
            "id": "titanic_" + dataset_type,
        }

    dataset = mldb.create_dataset(datasetConfig)
    def featProc(k, v):
        if k=="Cabin":
            return v[0]
        elif k in ["Pclass", "Age", "SibSp", "Parch", "Fare"]:
            return float(v)
        elif k == 'label':
            return int(v)
        return v

    ts = datetime.datetime.now()
    filename = mldb.plugin.get_plugin_dir() + "/titanic_%s.csv" % dataset_type
    for idx, csvLine in enumerate(csv.DictReader(open(filename))):
        tuples = [[k,featProc(k,v),ts] for k,v in csvLine.iteritems() if k != "PassengerId" and v!=""]
        dataset.record_row(csvLine["PassengerId"], tuples)

    # commit the dataset
    dataset.commit()

for cls_algo in ["glz", "dt", "bbdt"]:

    cls_fn_name = 'classifyFunction_' + cls_algo
    prb_fn_name = 'apply_probabilizer_' + cls_algo

    mldb.log(mldb.perform("PUT", "/v1/procedures/titanic_cls_train_%s" % cls_algo, [], {
        "type": "classifier.train",
        "params": {
            "trainingData": """
                SELECT {Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} AS features,
                       label = 1 AS label
                FROM titanic_train
                WHERE rowHash() % 5 != 1
                """,
            "algorithm": cls_algo,
            "functionName": cls_fn_name,
            "modelFileUrl": "file://models/titanic_%s.cls" % cls_algo,
        }
    }))

    mldb.log(mldb.perform("PUT", "/v1/procedures/titanic_prob_train_%s" % cls_algo, [], {
        "type": "probabilizer.train",
        "params": {
            "trainingData": """
                SELECT %s({{Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} AS features})[score] AS score,
                       label = 1 AS label
                FROM titanic_train
                WHERE rowHash() %% 5 = 1
                """ % cls_fn_name,
            "modelFileUrl": "file://models/probabilizer_" + cls_algo + ".json",
            "functionName": prb_fn_name,
        }
    }))

    mldb.log(mldb.perform("PUT", "/v1/functions/probabilizer_" + cls_algo, [], {
        "type": "sql.expression",
        "params": {
            "expression": "%s({%s({features}) AS *}) AS *" % (prb_fn_name, cls_fn_name)
        }
    }))

# setup static routes
mldb.plugin.serve_static_folder("/static", "static")
mldb.plugin.serve_documentation_folder('doc')
