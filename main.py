#
# main.py
# Francois Maillet, 2015-03-26
# Copyright (c) 2015 Datacratic Inc. All rights reserved.
#

import csv, datetime, json

mldb.log("Pytanic Plugin Executing setup...")


print mldb.perform("PUT", '/v1/procedures/import_titanic_raw', [], { 
    "type": "import.text",
    "params": { 
        "dataFileUrl": "http://public.mldb.ai/titanic_train.csv",
        "outputDataset": "titanic-train",
        "runOnCreation": True
    } 
})

for cls_algo in ["glz", "dt", "bbdt"]:

    print mldb.perform("PUT", "/v1/procedures/titanic_cls_train_%s" % cls_algo, [], {
        "type": "classifier.train",
        "params": {
            "trainingData": { 
                "select" : "{* EXCLUDING (Ticket, Name, label, Cabin)} as features, label = '1' as label",
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
                "select": "classifyFunction"+cls_algo+"( {{* EXCLUDING (label)} AS features})[(score)] as score, label = '1' as label",
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
