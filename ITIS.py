
# coding: utf-8

# This notebook works with the Taxonomic Information Registry. It picks up scientific names (currently only looking for those submitted by the SGCN process), queries the ITIS Solr service for matches, and caches a few specific properties in a key/value store. A lot of what was originally all in context here as functions has been moved out to a set of modules in a package. The following notes still make sense as to what this process is doing, but they reference what are now those modularized functions.
# 
# ### Clean the scientific name string for use in searches (bis.bis.cleanScientificName(name string))
# Note: I moved this function into the bis.bis module.
# 
# This is one of the more tricky areas of the process. People encode a lot of different signals into scientific names. If we clean too much out of the name string, we run the risk of not finding the taxon that they intended to provide. If we clean up too little, we won't find anything with the search. So far, for the SGCN case, we've decided to do the following in this code block for the purposes of finding the taxon in ITIS:
# 
# * Ignore population designations
# * Ignore strings after an "spp." designation
# * Set case for what appear to be species name strings to uppercase genus but lowercase everything else
# * Ignore text in between parentheses and brackets; these are often synonyms or alternate names that should be picked up from the ITIS record if we find a match
# 
# One thing that I deliberately did not do here was change cases where the name string includes signals like "sp." or "sp. 4". Those are seeming to indicate that the genus is known but species is not yet determined. Rather than strip this text and run the query, potentially resulting in a genus match, I opted to leave those strings in place, likely resulting in no match with ITIS. We may end up making a different design decision for the SGCN case and allow for matching to genus.
# 
# ### Package up the specific attributes we want to cache from ITIS (bis.itis.packageITISPairs(ITIS Solr JSON Doc))
# Note: I moved this and other ITIS functions into the bis.itis module.
# 
# This function takes the data coming from the ITIS service as JSON and pairs up the attributes and values we want to cache and use. The date/time stamp here for when the information is cached is vital metadata for determining usability. As soon as the information comes out of ITIS, it is potentially stale. The information we collect and use from ITIS through this process includes the following:
# * Discovered and accepted TSNs for the taxon
# * Taxonomic rank of the discovered taxon
# * Names with and without indicators for the discovered taxon
# * Taxonomic hierarchy with ranks (in the ITIS Solr service, this is always the accepted taxonomic hierarchy)
# * Vernacular names for the discovered taxon
# 
# ### Run the process for all supplied names
# The main process run below should eventually be the substance of a microservice on name matching. I set this up to create a local data structure (dictionary) for each record. The main point here is to set up the search, execute the search and package ITIS results, and then submit those for the record back to the Taxonomic Information Registry.
# 
# One of the major things we still need to work out here is what to do with updates over time. This script puts some record into the ITIS pairs whether we find a match or not. The query that gets names to run from the registration property looks for cases where the ITIS information is null (mostly because I needed to pick up where I left off when I found a few remaining issues that failed the script). We can then use cases where the "matchMethod" is "NotMatched" to go back and see if we can find name matches. This is particularly going to be the case where we find more than one match on a fuzzy search, which I still haven't dealt with.
# 
# We also need to figure out what to do when we want to update the information over time. With ITIS, once we have a matched TSN, we can then use that TSN to grab updates as they occur, including changes in taxonomy. But we need to figure out if we should change the structure of the TIR cache to keep all the old versions of what we found over time so that it can always be referred back to.

# In[2]:

import requests,re
from IPython.display import display
from bis import bis
from bis import itis
from bis import tir
from bis2 import gc2


# In[1]:

# Set up the actions/targets for this particular instance
thisRun = {}
thisRun["instance"] = "DataDistillery"
thisRun["db"] = "BCB"
thisRun["baseURL"] = gc2.sqlAPI(thisRun["instance"],thisRun["db"])
thisRun["commitToDB"] = True
thisRun["fuzzyLevel"] = "~0.5"
thisRun["totalRecordsToProcess"] = 500
thisRun["totalRecordsProcessed"] = 0

numberWithoutTIRData = 1

while numberWithoutTIRData == 1 and thisRun["totalRecordsProcessed"] <= thisRun["totalRecordsToProcess"]:

    q_recordToSearch = "SELECT id,         registration->'source' AS source,         registration->'followTaxonomy' AS followtaxonomy,         registration->'taxonomicLookupProperty' AS taxonomiclookupproperty,         registration->'scientificname' AS scientificname,         registration->'tsn' AS tsn         FROM tir.tir         WHERE itis IS NULL         LIMIT 1"
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
        thisRecord["itisPairs"] = itis.packageITISPairs(thisRecord["matchMethod"],thisRecord["matchString"],0)

        if thisRecord["taxonomicLookupProperty"] == "scientificname" and len(thisRecord["scientificname_search"]) != 0:

            # The ITIS Solr service does not fail in an elegant way, and so we need to try this whole section and except it out if the query fails
            try:
                thisRecord["itisSearchURL"] = itis.getITISSearchURL(thisRecord["scientificname_search"])

                # Try an exact match search
                itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
                thisRecord["numResults"] = len(itisSearchResults["response"]["docs"])

                # If we got only a single match on an exact match search, set the method and proceed
                if thisRecord["numResults"] == 1:
                    thisRecord["matchMethod"] = "Exact Match"

                # If we found nothing on an exact match search, try a fuzzy match
                elif thisRecord["numResults"] == 0:
                    itisSearchResults = requests.get(thisRecord["itisSearchURL"]+thisRun["fuzzyLevel"]).json()
                    thisRecord["numResults"] = len(itisSearchResults["response"]["docs"])
                    if thisRecord["numResults"] == 1:
                        thisRecord["matchMethod"] = "Fuzzy Match"

                # If there are results from exact or fuzzy match search, package the ITIS properties we want
                if thisRecord["numResults"] == 1:
                    thisRecord["itisPairs"] = itis.packageITISPairs(thisRecord["matchMethod"],thisRecord["matchString"],itisSearchResults["response"]["docs"][0])

                # Handle cases where discovered TSN usage is invalid by following accepted TSN
                if (thisRecord["itisPairs"].find('"usage"=>"valid"') == -1 or thisRecord["itisPairs"].find('"usage"=>"accepted"') == -1) and thisRecord["itisPairs"].find('"acceptedTSN"') > 0 and thisRecord["followTaxonomy"] == "True":
                    thisRecord["acceptedTSN"] = re.search('\"acceptedTSN"\=\>\"(.+?)\"', thisRecord["itisPairs"]).group(1)
                    thisRecord["discoveredTSN"] = re.search('\"tsn"\=\>\"(.+?)\"', thisRecord["itisPairs"]).group(1)

                    thisRecord["itisSearchURL"] = itis.getITISSearchURL(thisRecord["acceptedTSN"])
                    itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
                    thisRecord["numResults"] = len(itisSearchResults["response"]["docs"])
                    if thisRecord["numResults"] == 1:
                        thisRecord["matchMethod"] = "Followed Accepted TSN"
                        thisRecord["itisPairs"] = itis.packageITISPairs(thisRecord["matchMethod"],thisRecord["matchString"],itisSearchResults["response"]["docs"][0])
                        thisRecord["itisPairs"] = thisRecord["itisPairs"]+',"discoveredTSN"=>"'+thisRecord["discoveredTSN"]+'"'

            except Exception as e:
                print (e)
                pass

        elif thisRecord["taxonomicLookupProperty"] == "tsn" and thisRecord["tsn"] is not None:

            thisRecord["itisSearchURL"] = itis.getITISSearchURL(thisRecord["tsn"])

        display (thisRecord)
        if thisRun["commitToDB"]:
            print (tir.cacheToTIR(thisRun["baseURL"],thisRecord["id"],"itis",thisRecord["itisPairs"]))
        thisRun["totalRecordsProcessed"] = thisRun["totalRecordsProcessed"] + 1

        


# In[ ]:



