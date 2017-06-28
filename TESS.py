
# coding: utf-8

# This notebook used ITIS TSNs discovered and cached in the Taxonomic Information Registry to look for information from the USFWS Threatened and Endangered Species System web service. It cached either a negative result or a set of key/value pairs from the TESS service of interest in characterizing species in the TIR.

# In[7]:

import requests,re
from IPython.display import display
from bis import tir
from bis import tess
from bis2 import gc2


# In[20]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["totalRecordsToProcess"] = 500
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] <= thisRun["totalRecordsToProcess"]:
    
    q_recordToSearch = "SELECT id,         itis->'tsn' AS tsn,         itis->'acceptedTSN' AS acceptedtsn         FROM tir.tir         WHERE tess IS NULL         AND itis IS NOT NULL         AND itis->'itisMatchMethod' NOT LIKE 'Not Matched'         LIMIT 1"
    recordToSearch = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_recordToSearch).json()

    numberWithoutTIRData = len(recordToSearch["features"])

    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]

        thisRecord = {}
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["tsn"] = tirRecord["properties"]["tsn"]
        thisRecord["acceptedTSN"] = tirRecord["properties"]["acceptedtsn"]
        
        # Query based on discovered TSN and package data
        thisRecord["tessPairs"] = tess.packageTESSPairs(thisRecord["tsn"],tess.queryTESSbyTSN(thisRecord["tsn"]))

        # If no records are returned for the primary TSN, try the accepted TSN for the record
        if '"result"=>"none"' in thisRecord["tessPairs"] and type(thisRecord["acceptedTSN"]) is str and thisRecord["tsn"] != thisRecord["acceptedTSN"]:
            # Query based on discovered TSN and package data
            thisRecord["tessPairs"] = tess.packageTESSPairs(thisRecord["acceptedTSN"],tess.queryTESSbyTSN(thisRecord["acceptedTSN"]))

        display (thisRecord)
        if thisRun["commitToDB"]:
            print (tir.cacheToTIR(gc2.sqlAPI("DataDistillery","BCB"),thisRecord["id"],"tess",thisRecord["tessPairs"]))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# In[ ]:



