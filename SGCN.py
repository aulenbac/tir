
# coding: utf-8

# This notebook builds out a set of SGCN-specific annotations in the TIR based on configuration files housed on the SGCN source repository item in ScienceBase. It aligns taxonomic groups with a logical set of higher taxonomy names, setting all others to "other" if not found in the config file. It then uses a cached list of the original species names identified for the 2005 SWAP exercise to flag taxa that should be included in that list. We use the preferred taxonomic group in the national and state lists for display and filtering, and we use the hard list of 2005 species to flag them to the "National List" for consistency when our current process of checking taxonomic authorities (ITIS and WoRMS) does not turn up the names.

# In[1]:

import requests,json
from IPython.display import display
from datetime import datetime
import pandas as pd
from bis import tir
from bis2 import gc2
from bis import sgcn


# In[2]:

# Retrieve information from stored files on the SGCN base repository item
sb_sgcnCollectionItem = requests.get("https://www.sciencebase.gov/catalog/item/56d720ece4b015c306f442d5?format=json&fields=files").json()

for file in sb_sgcnCollectionItem["files"]:
    if file["title"] == "Configuration:Taxonomic Group Mappings":
        tgMappings = pd.read_table(file["url"], sep=",", encoding="utf-8")
    elif file["title"] == "Original 2005 SWAP National List for reference":
        swap2005 = pd.read_table(file["url"])

tgDict = {}
for index, row in tgMappings.iterrows():
    providedName = str(row["ProvidedName"])
    preferredName = str(row["PreferredName"])
    tgDict[providedName] = preferredName


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

    q_recordToSearch = "SELECT id, registration->>'scientificname' AS name_submitted, itis->>'nameWInd' AS name_itis, worms->>'valid_name' AS name_worms FROM tir.tir WHERE registration->>'source' = 'SGCN' AND sgcn->>'dateCached' IS NULL LIMIT 1"
    recordToSearch  = requests.get(thisRun["baseURL"]+"&q="+q_recordToSearch).json()

    numberWithoutTIRData = len(recordToSearch["features"])
    
    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]
    
        thisRecord = {}
        thisRecord["id"] = tirRecord["properties"]["id"]
        
        thisRecord["names"] = [tirRecord["properties"]["name_submitted"]]
        if tirRecord["properties"]["name_itis"] is not None and tirRecord["properties"]["name_itis"] not in thisRecord["names"]:
            thisRecord["names"].append(tirRecord["properties"]["name_itis"])
        if tirRecord["properties"]["name_worms"] is not None and tirRecord["properties"]["name_worms"] not in thisRecord["names"]:
            thisRecord["names"].append(tirRecord["properties"]["name_worms"])
        
        thisRecord["annotation"] = {}
        thisRecord["annotation"]["swap2005"] = False

        taxonomicgroup_submitted = sgcn.getSGCNTaxonomicGroup(thisRun["baseURL"],tirRecord["properties"]["name_submitted"])
        if taxonomicgroup_submitted in tgDict.keys():
            thisRecord["annotation"]["taxonomicgroup"] = tgDict[taxonomicgroup_submitted]
        elif taxonomicgroup_submitted in tgDict.values():
            thisRecord["annotation"]["taxonomicgroup"] = taxonomicgroup_submitted
        else:
            thisRecord["annotation"]["taxonomicgroup"] = "Other"

        for name in thisRecord["names"]:
            if name in list(swap2005["scientificname"]):
                thisRecord["annotation"]["swap2005"] = True
                break
                
        thisRecord["annotation"]["stateLists"] = sgcn.getSGCNStatesByYear(thisRun["baseURL"],tirRecord["properties"]["name_submitted"])
        thisRecord["annotation"]["dateCached"] = datetime.utcnow().isoformat()
        
        display (thisRecord)
        if thisRun["commitToDB"]:
            print (tir.cacheToTIR(thisRun["baseURL"],thisRecord["id"],"sgcn",json.dumps(thisRecord["annotation"])))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1


# In[ ]:



