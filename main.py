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
    print mldb.perform("DELETE", "/v1/datasets/titanic", [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/pipelines/titanic_cls_train_%s" % cls_algo, [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/pipelines/titanic_cls_test_%s" % cls_algo, [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/pipelines/titanic_prob_train_%s" % cls_algo, [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/blocks/classifyBlock%s" % cls_algo, [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/blocks/apply_probabilizer%s" % cls_algo, [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/blocks/probabilizer%s" % cls_algo, [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/blocks/explainBlock%s" % cls_algo, [["sync", "true"]], {})
    print mldb.perform("DELETE", "/v1/blocks/probabilizer%s" % cls_algo, [["sync", "true"]], {})



# create a mutable beh dataset
datasetConfig = {
        "type": "mutable",
        "id": "titanic",
        "address": "titanic.beh.gz"
    }

dataset = mldb.create_dataset(datasetConfig)

def featProc(k, v):
    if k=="Pclass": return "c"+v
    if k=="Cabin": return v[0]
    return v

ts = datetime.datetime.now()
for idx, csvLine in enumerate(csv.DictReader(open(mldb.plugin.get_plugin_dir() + "/titanic_train.csv"))):
    tuples = [[k,featProc(k,v),ts] for k,v in csvLine.iteritems() if k != "PassengerId" and v!=""]
    dataset.record_row(csvLine["PassengerId"], tuples)

# commit the dataset
dataset.commit()


######
# train a classifier
print "======= Train"
for cls_algo in cls_algos:
    trainClassifierPipelineConfig = {
        "id": "titanic_cls_train_"+cls_algo,
        "type": "classifier",
        "params": {
            "dataset": { "id": "titanic" },
            "algorithm": cls_algo,
            "classifierUri": "titanic_%s.cls" % cls_algo,
            "label": "label = '1'",
            "weight": "1.0",
            "where": "rowHash % 5 != 1",
            "select": "* EXCLUDING (Ticket, Name, label, Cabin)"
        }
    }

    print mldb.perform("PUT", "/v1/pipelines/titanic_cls_train_%s" % cls_algo,
            [["sync", "true"]], trainClassifierPipelineConfig)
    print mldb.perform("PUT", "/v1/pipelines/titanic_cls_train_%s/runs/1" % cls_algo,
            [["sync", "true"]], {})

######
# test the classifier
print "======= Test"
for cls_algo in cls_algos:
    applyBlockConfig = {
        "id": "classifyBlock" + cls_algo,
        "type": "classifier.apply",
        "params": {
            "classifierUri": "titanic_%s.cls" % cls_algo
        }
    }
    print mldb.perform("PUT", "/v1/blocks/classifyBlock%s" % cls_algo,
            [["sync", "true"]], applyBlockConfig)

    testClassifierPipelineConfig = {
        "id": "titanic_cls_test_%s" % cls_algo,
        "type": "accuracy",
        "params": {
            "dataset": { "id": "titanic" },
            "output": {
                "id": "cls_test_results_%s" % cls_algo,
                "type": "mutable",
                "address": "cls_test_results_%s.beh.gz" % cls_algo
            },
            "where": "rowHash % 5 = 1",
            "score": "APPLY BLOCK classifyBlock%s WITH (* EXCLUDING (label)) EXTRACT(score)" % cls_algo,
            "label": "label = '1'",
            "weight": "1.0"
        }
    }
    print mldb.perform("PUT", "/v1/pipelines/titanic_cls_test_%s" % cls_algo,
            [["sync", "true"]], testClassifierPipelineConfig)

    print mldb.perform("PUT", "/v1/pipelines/titanic_cls_test_%s/runs/1" % cls_algo,
            [["sync", "true"]], {})


    explBlockConfig = {
        "id": "explainBlock" + cls_algo,
        "type": "classifier.explain",
        "params": {
            "classifierUri": "titanic_%s.cls" % cls_algo
        }
    }
    print mldb.perform("PUT", "/v1/blocks/explainBlock%s" % cls_algo,
            [["sync", "true"]], explBlockConfig)


print "====== Train probabilizer"
for cls_algo in cls_algos:
    trainProbabilizerPipelineConfig = {
        "id": "titanic_prob_train_%s" % cls_algo,
        "type": "probabilizer",
        "params": {
            "dataset": { "id": "titanic" },
            "probabilizerUri": "probabilizer"+cls_algo+".json",
            # MAKES THIS FAIL!!
            #"select": "APPLY BLOCK classifyBlock"+cls_algo+" WITH (* EXCLUDING Ticket, Name, label, Cabin) EXTRACT (score)",
            "select": "APPLY BLOCK classifyBlock"+cls_algo+" WITH (* EXCLUDING (label)) EXTRACT (score)",
            "where": "rowHash() % 5 = 1",
            "label": "label = '1'",
        }
    };

    print mldb.perform("PUT", "/v1/pipelines/titanic_prob_train_%s" % cls_algo,
                    [["sync", "true"]], trainProbabilizerPipelineConfig)

    print mldb.perform("PUT", "/v1/pipelines/titanic_prob_train_%s/runs/1" % cls_algo,
                                      [["sync", "true"]], {})

    probabilizerBlockConfig = {
        "id": "probabilizer" + cls_algo,
        "type": "serial",
        "params": {
            "steps": [
                {
                    "id": "classifyBlock" + cls_algo
                },
                {
                    "id": "apply_probabilizer"+cls_algo,
                    "type": "probabilizer.apply",
                    "params": {
                        "probabilizerUri": "probabilizer"+cls_algo+".json"
                    }
                }
            ]
        }
    }
    print mldb.perform("PUT", "/v1/blocks/"+probabilizerBlockConfig["id"],
                        [["sync", "true"]], probabilizerBlockConfig)

# setup static routes
mldb.plugin.serve_static_folder("/static", "static")

def requestHandler(mldb, remaining, verb, resource, restParams, payload, contentType, contentLength, headers):
    global cls_algos
    print "Handling route in python"
    import json
    if verb == "GET" and remaining == "/multiApply":
        rez_accum = {}
        for algo in cls_algos:
            rez = mldb.perform("GET", "/v1/blocks/probabilizer"+algo+"/application", restParams, "{}")

            response = json.loads(rez["response"])
            rez_accum[algo] = response["pins"]["prob"]

        return rez_accum


mldb.plugin.set_request_handler(requestHandler)

