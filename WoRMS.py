
# coding: utf-8

# I'm experimenting with a new, smaller increment method here of working through the TIR registrations on WoRMS matching. In this process, we work on WoRMS matching until there is nothing left to work on by using a while loop on the number of possible records to work on. I also threw in a safeguard total number to process and include that in the check so the loop doesn't run away on us.
# 
# This might be a little less efficient than grabbing up a whole batch of records at once that meet some criteria and then processing through all of those. It has to execute the following three API interactions:
# 
# 1) Get a single TIR registration that does not currently have any WoRMS data and has been processed by ITIS (so we have an alternative name to work against)
# 2) Check the TIR registered name against the WoRMS REST service
# 3) If we find a match, insert the WoRMS information into the TIR
# 
# However, that is only a single additional API interaction per record, and it allows us to simply kick off this process whenever and have it run until there's nothing left to do. It seems like that might be conducive to processing on the Kafka/microservices architecture.

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
thisRun["totalRecordsToProcess"] = 5000
thisRun["totalRecordsProcessed"] = 0
thisRun["wormsNameService"] = "http://www.marinespecies.org/rest/AphiaRecordsByName/"
thisRun["wormsIDService"] = "http://www.marinespecies.org/rest/AphiaRecordByAphiaID/"

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] < thisRun["totalRecordsToProcess"]:
    q_recordToSearch = "SELECT id,         registration->>'source' AS source,         registration->>'followTaxonomy' AS followtaxonomy,         registration->>'taxonomicLookupProperty' AS taxonomiclookupproperty,         registration->>'scientificname' AS scientificname,         itis->>'nameWInd' AS nameWInd,         itis->>'nameWOInd' AS nameWOInd         FROM tir.tir         WHERE worms IS NULL         LIMIT 1"
    recordToSearch  = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_recordToSearch).json()
    display (recordToSearch)
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



