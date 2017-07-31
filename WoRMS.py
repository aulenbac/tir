
# coding: utf-8

# Like all of the current thinking for the TIR, this code works through every registered taxa in the Taxa Information Registry and attempts to find and cache information from the World Register of Marine Species (WoRMS). It does this with a while loop to find something processable in the TIR with a safeguard on the number of records to process at a time that can be set for "thisRun".
# 
# I also recently changed the whole data model for the TIR to accommodate JSON data structures in the different "buckets" of information we are caching rather than the key/value pairs in hstore columns. This lets us run a much more simple process here where we simply package a little bit of additional information and the eliminate (pop) a couple of properties from the WoRMS service response that we don't need/want to store. That is all handled in the worms module of the bis package with the packageWoRMSJSON function.
# 
# The query that runs here does try to get an ITIS name from the TIR for the registered taxa, and if it exists, it will potentially use that name to query if it is different from the registered name. That way, we take advantage of having run an ITIS match previously. I made this independent of whether or not an ITIS match has been found, so we can opt to run the process again once ITIS processing is completed or updated over time.

# In[1]:

import requests,re,json
from IPython.display import display
from bis import worms
from bis import bis
from bis import tir
from bis2 import gc2


# In[3]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["totalRecordsToProcess"] = 2000
thisRun["totalRecordsProcessed"] = 0
thisRun["wormsNameService"] = "http://www.marinespecies.org/rest/AphiaRecordsByName/"
thisRun["wormsIDService"] = "http://www.marinespecies.org/rest/AphiaRecordByAphiaID/"

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] < thisRun["totalRecordsToProcess"]:
    q_recordToSearch = "SELECT id,         registration->>'source' AS source,         registration->>'followTaxonomy' AS followtaxonomy,         registration->>'taxonomicLookupProperty' AS taxonomiclookupproperty,         registration->>'scientificname' AS scientificname,         itis->>'nameWInd' AS nameWInd,         itis->>'nameWOInd' AS nameWOInd         FROM tir.tir         WHERE worms IS NULL         LIMIT 1"
    recordToSearch  = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_recordToSearch).json()
    
    numberWithoutTIRData = len(recordToSearch["features"])

    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]

        # Set up a local data structure for storage and processing
        thisRecord = {}
        
        # Set data from query results
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["followTaxonomy"] = tirRecord["properties"]["followtaxonomy"]

        thisRecord["tryNames"] = []
        thisRecord["tryNames"].append(bis.cleanScientificName(tirRecord["properties"]["scientificname"]))
        if tirRecord["properties"]["namewind"] is not None and tirRecord["properties"]["namewind"] not in thisRecord["tryNames"]:
            thisRecord["tryNames"].append(tirRecord["properties"]["namewind"])
        if tirRecord["properties"]["namewoind"] is not None and tirRecord["properties"]["namewoind"] not in thisRecord["tryNames"]:
            thisRecord["tryNames"].append(tirRecord["properties"]["namewoind"])
        
        # Set defaults for thisRecord
        thisRecord["matchMethod"] = "Not Matched"
        wormsData = 0

        # Handle cases where cleaning the Scientific Name resulted in a single blank value to search on
        if len(thisRecord["tryNames"]) == 1 and len(thisRecord["tryNames"][0]) == 0:
            thisRecord["matchString"] = tirRecord["properties"]["scientificname"]
            thisRecord["tryNames"] = []

        for name in thisRecord["tryNames"]:
            # Handle the cases where there is enough interesting stuff in the scientific name string that it comes back blank from the cleaners
            if len(name) != 0:
                thisRecord["matchString"] = name
                thisRecord["baseQueryURL"] = thisRun["wormsNameService"]+name
                wormsSearchResults = requests.get(thisRecord["baseQueryURL"]+"?like=false&marine_only=false&offset=1")
                if wormsSearchResults.status_code == 204 or wormsSearchResults.json()[0]["valid_name"] is None:
                    wormsSearchResults = requests.get(thisRecord["baseQueryURL"]+"?like=true&marine_only=false&offset=1")
                    if wormsSearchResults.status_code != 204 and wormsSearchResults.json()[0]["valid_name"] is not None:
                        wormsData = wormsSearchResults.json()[0]
                        thisRecord["matchMethod"] = "Fuzzy Match"
                else:
                    wormsData = wormsSearchResults.json()[0]
                    thisRecord["matchMethod"] = "Exact Match"
                    break
        
        if not type(wormsData) == int and wormsData["status"] != "accepted" and thisRecord["followTaxonomy"] == "true":
            validAphiaID = str(wormsData["valid_AphiaID"])
            wormsSearchResults = requests.get(thisRun["wormsIDService"]+validAphiaID)
            if wormsSearchResults.status_code != 204 and wormsSearchResults.json()["valid_name"] is not None:
                wormsData = wormsSearchResults.json()
                thisRecord["matchString"] = validAphiaID
                thisRecord["matchMethod"] = "Followed Accepted AphiaID"
        
        thisRecord["wormsJSON"] = worms.packageWoRMSJSON(thisRecord["matchMethod"],thisRecord["matchString"],wormsData)
        display (thisRecord)
        if thisRun["commitToDB"]:
            print (tir.cacheToTIR(gc2.sqlAPI("DataDistillery","BCB"),thisRecord["id"],"worms",json.dumps(thisRecord["wormsJSON"])))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# In[4]:

print ("Number without WoRMS: "+str(requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q=SELECT count(*) AS num FROM tir.tir WHERE worms IS NULL").json()["features"][0]["properties"]["num"]))


# In[ ]:



