
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

# In[14]:

import requests,re
from IPython.display import display
from bis import worms
from bis import bis
from bis import tir
from bis2 import gc2


# In[19]:

numberWithoutWoRMS = 1
totalRecordsToProcess = 500
totalRecordsProcessed = 0

while numberWithoutWoRMS == 1 and totalRecordsProcessed <= totalRecordsToProcess:
    q_recordToSearch = "SELECT id,         registration->'scientificname' AS scientificname,         itis->'nameWInd' AS itisNameWInd         FROM tir.tir         WHERE worms IS NULL         AND itis IS NOT NULL         LIMIT 1"
    recordToSearch  = requests.get(gc2.sqlAPI("DataDistillery","BCB")+"&q="+q_recordToSearch).json()
    
    numberWithoutWoRMS = len(recordToSearch["features"])

    if numberWithoutWoRMS == 1:
        tirRecord = recordToSearch["features"][0]

        # Set up a local data structure for storage and processing
        thisRecord = {}
        
        # Set data from query results
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["scientificname_submitted"] = tirRecord["properties"]["scientificname"]
        thisRecord["scientificname_search"] = bis.cleanScientificName(thisRecord["scientificname_submitted"])
        thisRecord["itisNameWInd"] = tirRecord["properties"]["itisnamewind"]

        # Set defaults for thisRecord
        thisRecord["matchMethod"] = "Not Matched"
        thisRecord["matchString"] = thisRecord["scientificname_search"]
        wormsData = 0

        # Handle the cases where there is enough interesting stuff in the scientific name string that it comes back blank from the cleaners
        if len(thisRecord["scientificname_search"]) != 0:
            try:
                wormsSearchResults = requests.get("http://www.marinespecies.org/rest/AphiaRecordsByName/"+thisRecord["scientificname_search"]+"?like=false&marine_only=false&offset=1").json()
                thisRecord["matchMethod"] = "Exact Match"
                wormsData = wormsSearchResults[0]
            except:
                try:
                    wormsSearchResults = requests.get("http://www.marinespecies.org/rest/AphiaRecordsByName/"+thisRecord["scientificname_search"]+"?like=true&marine_only=false&offset=1").json()
                    thisRecord["matchMethod"] = "Fuzzy Match"
                    wormsData = wormsSearchResults[0]
                except:
                    if thisRecord["itisNameWInd"] != None and thisRecord["itisNameWInd"] != thisRecord["scientificname_search"]:
                        try:
                            wormsSearchResults = requests.get("http://www.marinespecies.org/rest/AphiaRecordsByName/"+thisRecord["itisNameWInd"]+"?like=false&marine_only=false&offset=1").json()
                            thisRecord["matchMethod"] = "ITIS Name Match"
                            wormsData = wormsSearchResults[0]
                        except:
                            pass

        thisRecord["wormsPairs"] = worms.packageWoRMSPairs(thisRecord["matchMethod"],wormsData)
        display (thisRecord)
        print (tir.cacheToTIR(gc2.sqlAPI("DataDistillery","BCB"),thisRecord["id"],"worms",thisRecord["wormsPairs"]))
        totalRecordsProcessed = totalRecordsProcessed + 1


# In[ ]:



