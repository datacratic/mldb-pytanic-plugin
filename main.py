#
# main.py
# Francois Maillet, 2015-03-26
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import csv, datetime, json

mldb.log("Pytanic Plugin Executing setup...")


cls_algos = ["glz", "dt", "bbdt"]
# cleanup in case things are already created
print "======= Cleanup"
for cls_algo in cls_algos:
    print mldb.perform("DELETE", "/v1/datasets/titanic-train", [], {})
    print mldb.perform("DELETE", "/v1/datasets/titanic-test", [], {})
    print mldb.perform("DELETE", "/v1/procedures/titanic_cls_train_%s" % cls_algo, [], {})
    print mldb.perform("DELETE", "/v1/procedures/titanic_cls_test_%s" % cls_algo, [], {})
    print mldb.perform("DELETE", "/v1/procedures/titanic_prob_train_%s" % cls_algo, [], {})
    print mldb.perform("DELETE", "/v1/functions/classifyFunction%s" % cls_algo, [], {})
    print mldb.perform("DELETE", "/v1/functions/apply_probabilizer%s" % cls_algo, [], {})
    print mldb.perform("DELETE", "/v1/functions/probabilizer%s" % cls_algo, [], {})
    print mldb.perform("DELETE", "/v1/functions/explainFunction%s" % cls_algo, [], {})
    print mldb.perform("DELETE", "/v1/functions/probabilizer%s" % cls_algo, [], {})



# load the train and test datasets
for dataset_type in ["train", "test"]:
    datasetConfig = {
            "type": "sparse.mutable",
            "id": "titanic-"+dataset_type,
        }

    dataset = mldb.create_dataset(datasetConfig)
    def featProc(k, v):
        if k=="Pclass": return "c"+v
        if k=="Cabin": return v[0]
        if k in ["Age", "SibSp", "Parch", "Fare"]: return float(v)
        return v

    ts = datetime.datetime.now()
    filename = mldb.plugin.get_plugin_dir() + "/titanic_%s.csv" % dataset_type
    for idx, csvLine in enumerate(csv.DictReader(open(filename))):
        tuples = [[k,featProc(k,v),ts] for k,v in csvLine.iteritems() if k != "PassengerId" and v!=""]
        dataset.record_row(csvLine["PassengerId"], tuples)

    # commit the dataset
    dataset.commit()


######
# train a classifier
print "======= Train"
for cls_algo in cls_algos:
    trainClassifierProcedureConfig = {
        "id": "titanic_cls_train_"+cls_algo,
        "type": "classifier.train",
        "params": {
            "trainingData": { 
                "select" : "{* EXCLUDING (Ticket, Name, label, Cabin)} as features, label = '1' as label",
                "from" : { "id": "titanic-train" },
                "where": "rowHash() % 5 != 1"
            },
            "algorithm": cls_algo,
            "configuration": {
                "bbdt": {
                    "type": "bagging",
                    "verbosity": 3,
                    "weak_learner": {
                        "type": "boosting",
                        "verbosity": 3,
                        "weak_learner": {
                            "type": "decision_tree",
                            "max_depth": 3,
                            "verbosity": 0,
                            "update_alg": "gentle",
                            "random_feature_propn": 0.5
                        },
                        "min_iter": 5,
                        "max_iter": 30
                    },
                    "num_bags": 5
                },
                "dt": {
                    "type": "decision_tree",
                    "max_depth": 8,
                    "verbosity": 3,
                    "update_alg": "prob"
                },
                "glz": {
                    "type": "glz",
                    "verbosity": 3,
                    "normalize ": " true",
                    "ridge_regression ": " true"
                }
            },
            "modelFileUrl": "file://models/titanic_%s.cls" % cls_algo,
            "weight": "1.0"
        }
    }

    print mldb.perform("PUT", "/v1/procedures/titanic_cls_train_%s" % cls_algo, [], trainClassifierProcedureConfig)
    print mldb.perform("PUT", "/v1/procedures/titanic_cls_train_%s/runs/1" % cls_algo, [], {})

######
# test the classifier
print "======= Test"
for cls_algo in cls_algos:
    applyFunctionConfig = {
        "id": "classifyFunction" + cls_algo,
        "type": "classifier",
        "params": {
            "modelFileUrl": "file://models/titanic_%s.cls" % cls_algo
        }
    }
    print mldb.perform("PUT", "/v1/functions/classifyFunction%s" % cls_algo, [], applyFunctionConfig)

    testClassifierProcedureConfig = {
        "id": "titanic_cls_test_%s" % cls_algo,
        "type": "classifier.test",
        "params": {
            "testingData": { 
                "select": "{*} as features, label = '1' as label",
                "from": {"id": "titanic-train" },
                "where": "rowHash() % 5 = 1"
            },            
            "outputDataset": { "id": "cls_test_results_%s" % cls_algo, "type": "sparse.mutable" },
            "score": "classifyFunction%s({ {* EXCLUDING (label)} AS features})[score]" % cls_algo,
            "weight": "1.0"
        }
    }
    print mldb.perform("PUT", "/v1/procedures/titanic_cls_test_%s" % cls_algo, [], testClassifierProcedureConfig)

    print mldb.perform("PUT", "/v1/procedures/titanic_cls_test_%s/runs/1" % cls_algo, [], {})


    explFunctionConfig = {
        "id": "explainFunction" + cls_algo,
        "type": "classifier.explain",
        "params": {
            "modelFileUrl": "file://models/titanic_%s.cls" % cls_algo
        }
    }
    print mldb.perform("PUT", "/v1/functions/explainFunction%s" % cls_algo, [], explFunctionConfig)


print "====== Train probabilizer"
for cls_algo in cls_algos:
    trainProbabilizerProcedureConfig = {
        "id": "titanic_prob_train_%s" % cls_algo,
        "type": "probabilizer.train",
        "params": {
            "trainingDataset": { "id": "titanic-train" },
            "modelFileUrl": "file://models/probabilizer"+cls_algo+".json",
            # MAKES THIS FAIL!!
            #"select": "classifyFunction"+cls_algo+"({* EXCLUDING Ticket, Name, label, Cabin})[score]",
            "select": "classifyFunction"+cls_algo+"( {{* EXCLUDING (label)} AS features})[(score)]",
            "where": "rowHash() % 5 = 1",
            "label": "label = '1'",
        }
    };

    print mldb.perform("PUT", "/v1/procedures/titanic_prob_train_%s" % cls_algo, [], trainProbabilizerProcedureConfig)

    print mldb.perform("PUT", "/v1/procedures/titanic_prob_train_%s/runs/1" % cls_algo, [], {})

    probabilizerFunctionConfig = {
        "id": "probabilizer" + cls_algo,
        "type": "serial",
        "params": {
            "steps": [
                {
                    "id": "classifyFunction" + cls_algo
                },
                {
                    "id": "apply_probabilizer"+cls_algo,
                    "type": "probabilizer",
                    "params": {
                        "modelFileUrl": "file://models/probabilizer"+cls_algo+".json"
                    }
                }
            ]
        }
    }
    print mldb.perform("PUT", "/v1/functions/"+probabilizerFunctionConfig["id"], [], probabilizerFunctionConfig)

# setup static routes
mldb.plugin.serve_static_folder("/static", "static")
mldb.plugin.serve_documentation_folder('doc')
