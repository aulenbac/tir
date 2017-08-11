
# coding: utf-8

# Since I've revamped all the TIR processors (yet again), I've eliminated the old notes here that are no longer fully relevant. This script now does what most of the TIR processors do, act continuously (or within a set limit) on every registered item in the TIR that does not yet have ITIS information cached.
# 
# I also significantly simplified this whole process by switching from the hstore to json data structure for the different "buckets" of cached information in the TIR. This allowed me to simply retrieve and process a matching document from the ITIS Solr service in its JSON format, pop out the properties that we don't want/need (or that were causing undue issues with the GC2 API and PostgreSQL), and repackage some of the information (hierarchy with ranks and vernacular names) into a more usable structure that takes advantage of JSON over a text string in need of constant parsing.
# 
# ### To Do
# Next, I need to add in a different route for this code that retrieves information from ITIS when the registration info in the TIR contains an already identified ITIS TSN. This will be for GAP species and other cases and will include not following the taxonomic information to a valid TSN, but simply recording when that is the case.

# In[1]:

import requests,json
from IPython.display import display
from bis import bis
from bis import itis
from bis import tir
from bis2 import gc2


# In[2]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["fuzzyLevel"] = "~0.5"
thisRun["totalRecordsToProcess"] = 1000
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] < thisRun["totalRecordsToProcess"]:

    q_recordToSearch = "SELECT id,         registration->>'source' AS source,         registration->>'followTaxonomy' AS followtaxonomy,         registration->>'taxonomicLookupProperty' AS taxonomiclookupproperty,         registration->>'scientificname' AS scientificname,         registration->>'tsn' AS tsn         FROM tir.tir         WHERE itis IS NULL         LIMIT 1"
    recordToSearch  = requests.get(thisRun["baseURL"]+"&q="+q_recordToSearch).json()
    
    numberWithoutTIRData = len(recordToSearch["features"])
    
    if numberWithoutTIRData == 1:
        tirRecord = recordToSearch["features"][0]
        
        # Set up a local data structure for storage and processing
        thisRecord = {}

        # Set data from query results
        thisRecord["id"] = tirRecord["properties"]["id"]
        thisRecord["source"] = tirRecord["properties"]["source"]
        thisRecord["followTaxonomy"] = tirRecord["properties"]["followtaxonomy"]
        thisRecord["taxonomicLookupProperty"] = tirRecord["properties"]["taxonomiclookupproperty"]
        thisRecord["tsn"] = tirRecord["properties"]["tsn"]
        thisRecord["scientificname"] = tirRecord["properties"]["scientificname"]
        thisRecord["scientificname_search"] = bis.cleanScientificName(thisRecord["scientificname"])

        # Set defaults for thisRecord
        thisRecord["matchMethod"] = "Not Matched"
        thisRecord["matchString"] = thisRecord["scientificname_search"]
        thisRecord["itisData"] = itis.packageITISJSON(thisRecord["matchMethod"],thisRecord["matchString"],0)
        thisRecord["numResults"] = 0
        itisDoc = {}

        if thisRecord["taxonomicLookupProperty"] == "scientificname" and len(thisRecord["scientificname_search"]) != 0:

            thisRecord["itisSearchURL"] = itis.getITISSearchURL(thisRecord["scientificname_search"],False)

            # Try an exact match search
            try:
                itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
                thisRecord["numResults"] = len(itisSearchResults["response"]["docs"])
            except Exception as e:
                print (e)
                pass


            # If we got only a single record on an exact match search, set the method and proceed
            if thisRecord["numResults"] == 1:
                thisRecord["matchMethod"] = "Exact Match"
                itisDoc = itisSearchResults["response"]["docs"][0]

            # If we found nothing on an exact match search, try a fuzzy match
            elif thisRecord["numResults"] == 0:
                try:
                    itisSearchResults = requests.get(thisRecord["itisSearchURL"]+thisRun["fuzzyLevel"]).json()
                    thisRecord["numResults"] = len(itisSearchResults["response"]["docs"])
                except Exception as e:
                    print (e)
                    pass
                if thisRecord["numResults"] == 1:
                    thisRecord["matchMethod"] = "Fuzzy Match"
                    itisDoc = itisSearchResults["response"]["docs"][0]

            # If we got a result but the usage is not accepted/invalid and we should follow taxonomy for this record, then retrieve the record for the accepted TSN
            if len(itisDoc) > 0 and itisDoc["usage"] in ["not accepted","invalid"] and thisRecord["followTaxonomy"]:
                thisRecord["itisSearchURL"] = itis.getITISSearchURL(itisDoc["acceptedTSN"][0],False)
                try:
                    itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
                except Exception as e:
                    print (e)
                    pass
                if thisRecord["numResults"] == 1:
                    thisRecord["matchMethod"] = "Followed Accepted TSN"
                    itisDoc = itisSearchResults["response"]["docs"][0]

            # If we got an ITIS Doc returned, package the results
            if len(itisDoc) > 0:
                thisRecord["itisData"] = itis.packageITISJSON(thisRecord["matchMethod"],thisRecord["matchString"],itisDoc)

        elif thisRecord["taxonomicLookupProperty"] == "tsn" and thisRecord["tsn"] is not None:
            thisRecord["itisSearchURL"] = itis.getITISSearchURL(thisRecord["tsn"],False)
            itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
            thisRecord["matchMethod"] = "TSN Query"
            thisRecord["matchString"] = thisRecord["tsn"]
            itisDoc = itisSearchResults["response"]["docs"][0]
            thisRecord["itisData"] = itis.packageITISJSON(thisRecord["matchMethod"],thisRecord["matchString"],itisDoc)
            
        display (thisRecord)
        if thisRun["commitToDB"]:
            print (tir.cacheToTIR(thisRun["baseURL"],thisRecord["id"],"itis",json.dumps(thisRecord["itisData"])))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1

        


# In[ ]:



