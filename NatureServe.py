
# coding: utf-8

# This notebook uses registered scientific names and taxon-mathed names from ITIS and/or WoRMS, looks for those names from the NatureServe species service, and returns a simplified data structure for caching in the Taxonomic Information Registry and use within our systems. It is set up to loop the entire TIR for everything without NatureServe data until finished (with a safeguard on total records processed).
# 
# There is a lot of complexity to the species details returned from the NatureServe service. What we are interested right now for the TIR is only the conservation status information - global, US national, and US state. We pull these specific codes into the TIR so that we can use them as facets/filters in applications of the Biogeographic Information System such as comparing State SGCN listings to State Heritage Society listings.

# In[1]:

import requests,json
from IPython.display import display
from bis import natureserve
from bis import tir
from bis2 import gc2
from bis2 import natureserve as natureservekeys


# In[2]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["totalRecordsToProcess"] = 200
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] < thisRun["totalRecordsToProcess"]:

    q_recordToSearch = "SELECT id,         registration->>'scientificname' AS name_registered,         itis->>'nameWInd' AS name_itis,         worms->>'valid_name' AS name_worms         FROM tir.tir         WHERE natureserve IS NULL         LIMIT 1"
    recordToSearch  = requests.get(thisRun["baseURL"]+"&q="+q_recordToSearch).json()
    
    numberWithoutTIRData = len(recordToSearch["features"])
    
    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]

        thisRecord = {}
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["name_registered"] = tirRecord["properties"]["name_registered"]
        thisRecord["name_itis"] = tirRecord["properties"]["name_itis"]
        thisRecord["name_worms"] = tirRecord["properties"]["name_worms"]

        thisRecord["tryNames"] = []
        thisRecord["tryNames"].append(thisRecord["name_registered"])
        if thisRecord["name_itis"] is not None and thisRecord["name_itis"] not in thisRecord["tryNames"]:
            thisRecord["tryNames"].append(thisRecord["name_itis"])
        if thisRecord["name_worms"] is not None and thisRecord["name_worms"] not in thisRecord["tryNames"]:
            thisRecord["tryNames"].append(thisRecord["name_worms"])
        
        for name in thisRecord["tryNames"]:
            thisRecord["elementGlobalID"] = natureserve.queryNatureServeID(name)
            if thisRecord["elementGlobalID"] is not None:
                break

        # Run the function to query and package NatureServe data
        thisRecord["natureServeData"] = natureserve.packageNatureServeJSON(natureservekeys.speciesAPI(),thisRecord["elementGlobalID"])

        # Display the record, cache results, and show query status
        display (thisRecord)
        if thisRun["commitToDB"]:
            print (tir.cacheToTIR(thisRun["baseURL"],thisRecord["id"],"natureserve",json.dumps(thisRecord["natureServeData"])))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# In[ ]:



