
# coding: utf-8

# This notebook works through Taxa Information Registry (TIR) records until all are processed to search for and cache information from the USFWS Threatened and Endangered Species System. It relies on a processing function in the tess module of the bis package that sets up and runs the query against the [TESS web service](https://ecos.fws.gov/ecp/species-query) and returns a dictionary of properties that are converted to a JSON string and cached in the TIR. Like all of the TIR processors, this code always returns at least a negative query result for caching in the TIR so that we know the record was checked at a particular date and time.
# 
# The TESS service returns what is basically a set of rows from a database table or view that repeats high level information for each species along with one or more listing status records. To make this a little cleaner for our purposes, the tess module's tessQuery function creates a single JSON structure (Python dictionary) with the high level information as first order properties and listingStatus as a list of one or more listing status values.
# 
# The tessQuery function uses ITIS TSNs when available for a TIR record as the primary search mechanism and then will use available scientific names from the registration data or from ITIS or WoRMS in an attempt to find a possible match.

# In[3]:

import requests,json
from IPython.display import display
from bis import tir
from bis import tess
from bis2 import gc2


# In[4]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["totalRecordsToProcess"] = 5000
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] < thisRun["totalRecordsToProcess"]:
    
    q_recordToSearch = "SELECT id,         registration->>'scientificname' AS name_state,         itis->>'itisMatchMethod' AS matchmethod_itis,         itis->>'tsn' AS tsn,         itis->>'acceptedTSN' AS acceptedtsn,         itis->>'nameWInd' AS name_itis,         worms->>'MatchMethod' AS matchmethod_worms,         worms->>'valid_name' AS name_worms         FROM tir.tir         WHERE tess IS NULL         LIMIT 1"
    recordToSearch = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_recordToSearch).json()

    numberWithoutTIRData = len(recordToSearch["features"])

    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]

        thisRecord = {}
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["tsnsToSearch"] = []
        thisRecord["namesToSearch"] = [tirRecord["properties"]["name_state"]]
        thisRecord["tessJSON"] = tess.queryTESS()

        if tirRecord["properties"]["matchmethod_itis"] not in [None,"Not Matched"]:
            if tirRecord["properties"]["tsn"] is not None:
                thisRecord["tsnsToSearch"].append(tirRecord["properties"]["tsn"])
            if tirRecord["properties"]["acceptedtsn"] not in [None,thisRecord["tsnsToSearch"]]:
                thisRecord["tsnsToSearch"].append(tirRecord["properties"]["acceptedtsn"])
            if tirRecord["properties"]["name_itis"] not in [None,thisRecord["namesToSearch"]]:
                thisRecord["namesToSearch"].append(tirRecord["properties"]["name_itis"])
        
        if tirRecord["properties"]["matchmethod_worms"] not in [None,"Not Matched"]:
            if tirRecord["properties"]["name_worms"] not in [None,thisRecord["namesToSearch"]]:
                thisRecord["namesToSearch"].append(tirRecord["properties"]["name_worms"])

        if len(thisRecord["tsnsToSearch"]) > 0:
            for tsn in thisRecord["tsnsToSearch"]:
                thisRecord["tessJSON"] = tess.queryTESS("TSN",tsn)
                if thisRecord["tessJSON"]["result"]:
                    break

        if not thisRecord["tessJSON"]["result"] and len(thisRecord["namesToSearch"]) > 0:
            for name in thisRecord["namesToSearch"]:
                thisRecord["tessJSON"] = tess.queryTESS("SCINAME",name)
                if thisRecord["tessJSON"]["result"]:
                    break

        display (thisRecord["tessJSON"])
        if thisRun["commitToDB"]:
            r = tir.cacheToTIR(thisRun["baseURL"],thisRecord["id"],"tess",json.dumps(thisRecord["tessJSON"]))
            print (r)
            if not r["success"]:
                thisRecord["tessJSON"].pop("REFUGE_OCCURRENCE",None)
                print (tir.cacheToTIR(thisRun["baseURL"],thisRecord["id"],"tess",json.dumps(thisRecord["tessJSON"])))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# In[ ]:



