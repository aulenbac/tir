
# coding: utf-8

# After dealing with a bunch of issues in trying to build out union selects for views in the PostgreSQL instance for the SGCN National List and others, I decided to come back to putting the main logic for what we need to get out of the data into code that writes values back into the database. I settled on the following common TIR concepts that I think should transcend multiple types of registrations:
# * Scientific Name - the core scientific name that the taxon is known by with the taxonomic authorities preferred
# * Common Name - a common name for a taxon that comes from one of multiple sources
# * Rank - taxonomic rank in the hierarchy for the supplied scientific name (applies in cases where the taxon was aligned with a taxonomic authority)
# * Taxonomic Group - a logical name for a group of taxa, not necessarily tied directly to official taxonomy
# * Match Method - the method that was successful in matching a scientific name to a taxonomic authority (helps tease out records that were not matched)
# * Taxonomic Authority ID - a unique identifier (usually a URL/URI) for the record
# * Source - the source of the original registration (used especially as a filtering parameter to tease out only certain records)

# In[1]:

import requests,json
from IPython.display import display
from datetime import datetime
from bis2 import gc2
from bis import bis
from bis import sgcn


# In[3]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["totalRecordsToProcess"] = 1000
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] < thisRun["totalRecordsToProcess"]:
    q_recordToSearch = "SELECT *         FROM tir.tir         WHERE itis IS NOT NULL         AND worms IS NOT NULL         AND (cachedate < (itis->>'cacheDate')::date         OR cachedate < (worms->>'cacheDate')::date         OR cachedate < (sgcn->>'cacheDate')::date)         LIMIT 1"
    recordToSearch = requests.get(thisRun["baseURL"]+"&q="+q_recordToSearch).json()

    numberWithoutTIRData = len(recordToSearch["features"])

    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]

        thisRecord = {}
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["registration"] = json.loads(tirRecord["properties"]["registration"])
        thisRecord["itis"] = json.loads(tirRecord["properties"]["itis"])
        thisRecord["worms"] = json.loads(tirRecord["properties"]["worms"])
        if tirRecord["properties"]["sgcn"] is not None:
            thisRecord["sgcn"] = json.loads(tirRecord["properties"]["sgcn"])
        _source = thisRecord["registration"]["source"]

        tirCommon = {}
        tirCommon["commonname"] = None
        tirCommon["authorityid"] = None
        tirCommon["rank"] = None
        tirCommon["matchmethod"] = None
        tirCommon["taxonomicgroup"] = None
        tirCommon["cachedate"] = datetime.utcnow().isoformat()
        
        tirCommon["scientificname"] = bis.stringCleaning(thisRecord["registration"]["scientificname"])
        tirCommon["source"] = thisRecord["registration"]["source"]
        tirCommon["matchmethod"] = "Not Matched"
        tirCommon["authorityid"] = "Not Matched to Taxonomic Authority"
        tirCommon["rank"] = "Unknown Taxonomic Rank"

        if thisRecord["itis"]["MatchMethod"] != "Not Matched":
            tirCommon["scientificname"] = thisRecord["itis"]["nameWInd"]
            tirCommon["matchmethod"] = thisRecord["itis"]["MatchMethod"]
            tirCommon["authorityid"] = "http://services.itis.gov/?q=tsn:"+str(thisRecord["itis"]["tsn"])
            tirCommon["rank"] = thisRecord["itis"]["rank"]
        elif thisRecord["worms"]["MatchMethod"] != "Not Matched":
            tirCommon["scientificname"] = thisRecord["worms"]["valid_name"]
            tirCommon["matchmethod"] = thisRecord["worms"]["MatchMethod"]
            tirCommon["authorityid"] = "http://www.marinespecies.org/rest/AphiaRecordsByName/"+str(thisRecord["worms"]["AphiaID"])
            tirCommon["rank"] = thisRecord["worms"]["rank"]
            
        if tirCommon["commonname"] is None and _source == 'SGCN':
            tirCommon["commonname"] = sgcn.getSGCNCommonName(thisRun["baseURL"],bis.stringCleaning(thisRecord["registration"]["scientificname"]))

        if tirCommon["commonname"] is None and "commonnames" in list(thisRecord["itis"].keys()):
            for name in thisRecord["itis"]["commonnames"]:
                if name["language"] == "English" or name["language"] == "unspecified":
                    tirCommon["commonname"] = name["name"]
                    break
        
        if tirCommon["commonname"] is None:
            tirCommon["commonname"] = "no common name"

        if _source == "SGCN" and "sgcn" in list(thisRecord.keys()):
            tirCommon["taxonomicgroup"] = thisRecord["sgcn"]["taxonomicgroup"]
            
            if tirCommon["matchmethod"] == "Not Matched" and "swap2005" in list(thisRecord["sgcn"].keys()) and thisRecord["sgcn"]["swap2005"] is True:
                tirCommon["matchmethod"] = "Legacy Match"
                tirCommon["authorityid"] = "https://www.sciencebase.gov/catalog/file/get/56d720ece4b015c306f442d5?f=__disk__38%2F22%2F26%2F38222632f48bf0c893ad1017f6ba557d0f672432"
        elif _source == "GAP Species":
            tirCommon["taxonomicgroup"] = thisRecord["registration"]["taxonomicgroup"]
            
            if tirCommon["scientificname"] != thisRecord["registration"]["scientificname"]:
                tirCommon["scientificname"] = thisRecord["registration"]["scientificname"]
            if tirCommon["commonname"] != thisRecord["registration"]["commonname"]:
                tirCommon["commonname"] = thisRecord["registration"]["commonname"]
        else:
            tirCommon["taxonomicgroup"] = "Other"

        display (tirCommon)
        if thisRun["commitToDB"]:
            q_tirCommon = "UPDATE tir.tir SET                 source='"+tirCommon["source"]+"',                 scientificname='"+tirCommon["scientificname"]+"',                 commonname='"+bis.stringCleaning(tirCommon["commonname"])+"',                 authorityid='"+tirCommon["authorityid"]+"',                 rank='"+tirCommon["rank"]+"',                 taxonomicgroup='"+tirCommon["taxonomicgroup"]+"',                 matchmethod='"+tirCommon["matchmethod"]+"',                 cachedate='"+tirCommon["cachedate"]+"'                 WHERE id = "+str(thisRecord["id"])
            print (requests.get(thisRun["baseURL"]+"&q="+q_tirCommon).json())
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1
        


# In[ ]:



