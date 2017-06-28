
# coding: utf-8

# This notebook uses submitted scientific names and taxon-mathed names from ITIS, looks for those names from the NatureServe species services, and returns key/value pairs for caching in the Taxonomic Information Registry and use within our systems. It is set up to loop the entire TIR for everything without NatureServe data until finished (with a safeguard on total records processed). We eventually need to deal with the issue of whether or not we need to cache this information over time or flush and rebuild our cache periodically.

# In[6]:

import requests
from IPython.display import display
from bis import natureserve
from bis import tir
from bis2 import gc2
from bis2 import natureserve as natureservekeys


# In[7]:

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

    q_recordToSearch = "SELECT id,         registration->'scientificname' AS scientificname,         itis->'nameWInd' AS namewind,         itis->'nameWOInd' AS namewoind         FROM tir.tir         WHERE natureserve IS NULL         LIMIT 1"
    recordToSearch  = requests.get(thisRun["baseURL"]+"&q="+q_recordToSearch).json()
    
    numberWithoutTIRData = len(recordToSearch["features"])
    
    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]

        thisRecord = {}
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["scientificname"] = tirRecord["properties"]["scientificname"]
        thisRecord["nameWOInd"] = tirRecord["properties"]["namewoind"]
        thisRecord["nameWInd"] = tirRecord["properties"]["namewind"]

        thisRecord["tryNames"] = []
        thisRecord["tryNames"].append(thisRecord["scientificname"])
        if thisRecord["nameWInd"] is not None and thisRecord["nameWInd"] not in thisRecord["tryNames"]:
            thisRecord["tryNames"].append(thisRecord["nameWInd"])
        if thisRecord["nameWOInd"] is not None and thisRecord["nameWOInd"] not in thisRecord["tryNames"]:
            thisRecord["tryNames"].append(thisRecord["nameWOInd"])
        
        for name in thisRecord["tryNames"]:
            thisRecord["elementGlobalID"] = natureserve.queryNatureServeID(name)
            if thisRecord["elementGlobalID"] != "none":
                break

        # Run the function to query and pacage NatureServe key/value pairs
        thisRecord["natureServePairs"] = natureserve.packageNatureServePairs(natureservekeys.speciesAPI(),thisRecord["elementGlobalID"])

        # Display the record, cache results, and show query status
        display (thisRecord)
        if thisRun["commitToDB"]:
            print (tir.cacheToTIR(thisRun["baseURL"],thisRecord["id"],"natureserve",thisRecord["natureServePairs"]))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# In[ ]:



