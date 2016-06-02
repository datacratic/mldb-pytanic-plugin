#
# main.py
# Francois Maillet, 2015-03-26
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import csv, datetime, json

mldb.log("Pytanic Plugin Executing setup...")


print mldb.perform("DELETE", "/v1/datasets/titanic-train", [], {})
print mldb.perform("DELETE", "/v1/datasets/titanic-test", [], {})

# load the train and test datasets
for dataset_type in ["train", "test"]:
    datasetConfig = {
            "type": "sparse.mutable",
            "id": "titanic-"+dataset_type,
        }

    dataset = mldb.create_dataset(datasetConfig)
    def featProc(k, v):
        if k=="Cabin": return v[0]
        if k in ["Pclass", "Age", "SibSp", "Parch", "Fare"]: return float(v)
        return v

    ts = datetime.datetime.now()
    filename = mldb.plugin.get_plugin_dir() + "/titanic_%s.csv" % dataset_type
    for idx, csvLine in enumerate(csv.DictReader(open(filename))):
        tuples = [[k,featProc(k,v),ts] for k,v in csvLine.iteritems() if k != "PassengerId" and v!=""]
        dataset.record_row(csvLine["PassengerId"], tuples)

    # commit the dataset
    dataset.commit()

for cls_algo in ["glz", "dt", "bbdt"]:

    print mldb.perform("PUT", "/v1/procedures/titanic_cls_train_%s" % cls_algo, [], {
        "type": "classifier.train",
        "params": {
            "trainingData": { 
                "select" : "{Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} as features, label = '1' as label",
                "from" : { "id": "titanic-train" },
                "where": "rowHash() % 5 != 1"
            },
            "algorithm": cls_algo,
            "functionName":  "classifyFunction"+cls_algo,
            "modelFileUrl": "file://models/titanic_%s.cls" % cls_algo,
            "runOnCreation": True
        }
    })
    
    print mldb.perform("PUT", "/v1/procedures/titanic_prob_train_%s" % cls_algo, [], {
        "type": "probabilizer.train",
        "params": {
            "trainingData": { 
                "select": "classifyFunction"+cls_algo+"( {{Sex, Age, Fare, Embarked, Parch, SibSp, Pclass} AS features})[score] as score, label = '1' as label",
                "from": { "id": "titanic-train" },
                "where": "rowHash() % 5 = 1"
            },
            "modelFileUrl": "file://models/probabilizer"+cls_algo+".json",
            "functionName": "apply_probabilizer"+cls_algo,
            "runOnCreation": True
        }
    })
    
    print mldb.perform("PUT", "/v1/functions/probabilizer" + cls_algo, [], {
        "type": "sql.expression",
        "params": {
            "expression": "apply_probabilizer%s({classifyFunction%s({features}) as *}) as *" % (cls_algo, cls_algo)
        }
    })

# setup static routes
mldb.plugin.serve_static_folder("/static", "static")
mldb.plugin.serve_documentation_folder('doc')
