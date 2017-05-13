
# coding: utf-8

# This notebook works with the Taxonomic Information Registry. It picks up scientific names (currently only looking for those submitted by the SGCN process), queries the ITIS Solr service for matches, and caches a few specific properties in a key/value store.

# In[1]:

import requests,configparser,re
from IPython.display import display


# In[2]:

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))


# In[3]:

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL


# ### Clean the scientific name string for use in searches
# This is one of the more tricky areas of the process. People encode a lot of different signals into scientific names. If we clean too much out of the name string, we run the risk of not finding the taxon that they intended to provide. If we clean up too little, we won't find anything with the search. So far, for the SGCN case, we've decided to do the following in this code block for the purposes of finding the taxon in ITIS:
# 
# * Ignore population designations
# * Ignore strings after an "spp." designation
# * Set case for what appear to be species name strings to uppercase genus but lowercase everything else
# * Ignore text in between parentheses and brackets; these are often synonyms or alternate names that should be picked up from the ITIS record if we find a match
# 
# One thing that I deliberately did not do here was change cases where the name string includes signals like "sp." or "sp. 4". Those are seeming to indicate that the genus is known but species is not yet determined. Rather than strip this text and run the query, potentially resulting in a genus match, I opted to leave those strings in place, likely resulting in no match with ITIS. We may end up making a different design decision for the SGCN case and allow for matching to genus. 

# In[32]:

# There are a few things that we've found in the name strings that, if removed or modified, will result in a valid taxon name string for the ITIS service
def cleanScientificName(scientificname):
    # Clean up all upper case strings because the ITIS service doesn't like them
    if any(x.isupper() for x in scientificname[-(len(scientificname)-1):]):
        scientificname = scientificname.lower().capitalize()

    # Replace "subsp." with "ssp." in order to make the subspecies search work
    scientificname = scientificname.replace("subsp.", "ssp.")
    
    # Get rid of text in parens and brackets; this is a design decision to potentially do away with information that might be important, but it gets retained in the original records
    scientificname = re.sub("[\(\[].*?[\)\]]", "", scientificname)
    
    # Get rid of characters after certain characters to produce a shorter (and sometimes higher taxonomy) name string compatible with the ITIS service
    afterChars = ["(", " sp.", " spp.", " sp ", " spp ", " n.", " pop."]
    # Add a space at the end for this step to help tease out issues with split characters ending the string (could probably be better as re)
    scientificname = scientificname+" "
    for char in afterChars:
        scientificname = scientificname.split(char, 1)[0]

    # Final cleanup of extra spaces
    scientificname = " ".join(scientificname.split())

    return scientificname


# In[33]:

def getITISSearchURL(searchstr):
    # Default to using name without indicator as the search term
    itisTerm = "nameWOInd"
    
    # "var." and "ssp." indicate that the string has population and variation indicators and should use the WInd service
    if searchstr.find("var.") > 0 or searchstr.find("ssp."):
        itisTerm = "nameWInd"
    
    try:
        int(searchstr)
        itisTerm = "tsn"
    except:
        pass
    
    # Put the search term together with the scientific name value including the weird escape character sequence that ITIS wants
    return "http://services.itis.gov/?wt=json&rows=1&q="+itisTerm+":"+searchstr.replace(" ","\%20")


# ### Package up the specific attributes we want to cache from ITIS
# This function takes the data coming from the ITIS service as JSON and pairs up the attributes and values we want to cache and use. The date/time stamp here for when the information is cached is vital metadata for determining usability. As soon as the information comes out of ITIS, it is potentially stale. The information we collect and use from ITIS through this process includes the following:
# * Discovered and accepted TSNs for the taxon
# * Taxonomic rank of the discovered taxon
# * Names with and without indicators for the discovered taxon
# * Taxonomic hierarchy with ranks (in the ITIS Solr service, this is always the accepted taxonomic hierarchy)
# * Vernacular names for the discovered taxon

# In[6]:

def packageITISPairs(matchMethod,itisJSON):
    import datetime
    dt = datetime.datetime.utcnow().isoformat()
    itisPairs = '"cacheDate"=>"'+dt+'"'
    itisPairs = itisPairs+',"itisMatchMethod"=>"'+matchMethod+'"'

    if type(itisJSON) is int:
        return itisPairs
    else:
        itisPairs = itisPairs+',"createDate"=>"'+itisJSON['response']['docs'][0]['createDate']+'"'
        itisPairs = itisPairs+',"updateDate"=>"'+itisJSON['response']['docs'][0]['updateDate']+'"'
        itisPairs = itisPairs+',"tsn"=>"'+itisJSON['response']['docs'][0]['tsn']+'"'
        itisPairs = itisPairs+',"rank"=>"'+itisJSON['response']['docs'][0]['rank']+'"'
        itisPairs = itisPairs+',"nameWInd"=>"'+itisJSON['response']['docs'][0]['nameWInd']+'"'
        itisPairs = itisPairs+',"nameWOInd"=>"'+itisJSON['response']['docs'][0]['nameWOInd']+'"'
        itisPairs = itisPairs+',"usage"=>"'+itisJSON['response']['docs'][0]['usage']+'"'

        if 'acceptedTSN' in itisJSON['response']['docs'][0]:
            itisPairs = itisPairs+',"acceptedTSN"=>"'+itisJSON['response']['docs'][0]['acceptedTSN'][0]+'"'

        hierarchy = itisJSON['response']['docs'][0]['hierarchySoFarWRanks'][0]
        hierarchy = hierarchy[hierarchy.find(':$')+2:-1]
        hierarchy = '"'+hierarchy.replace(':', '"=>"').replace('$', '","')+'"'
        itisPairs = itisPairs+','+hierarchy

        if "vernacular" in itisJSON['response']['docs'][0]:
            vernacularList = []
            for commonName in itisJSON['response']['docs'][0]['vernacular']:
                commonNameElements = commonName.split('$')
                vernacularList.append('"vernacular:'+commonNameElements[2]+'"=>"'+commonNameElements[1]+'"')
            strVernacularList = ''.join(vernacularList).replace("\'", "''").replace('""','","')
            itisPairs = itisPairs+','+strVernacularList

        return itisPairs


# In[7]:

# Basic function to insert subject ID, property, and value into tircache
def cacheToTIR(gid,infotype,pairs):
    import requests
    updateQ = "UPDATE tir.tir2 SET "+infotype+" = '"+pairs+"' WHERE gid = "+str(gid)
    r = requests.get(getBaseURL()+"&q="+updateQ).json()
    return r


# In[43]:

# Query for the registered names we want to run through the system
uniqueNames  = requests.get(getBaseURL()+"&q=SELECT gid,registration->'SGCN_ScientificName_Submitted' AS scientificname FROM tir.tir2 WHERE itis IS NULL").json()


# ### Run the process for all supplied names
# This is the process that should eventually be the substance of a microservice on name matching. I set this up to create a local data structure (dictionary) for each record. The main point here is to set up the search, execute the search and package ITIS results, and then submit those for the record back to the Taxonomic Information Registry.
# 
# One of the major things we still need to work out here is what to do with updates over time. This script puts some record into the ITIS pairs whether we find a match or not. The query that gets names to run from the registration property looks for cases where the ITIS information is null (mostly because I needed to pick up where I left off when I found a few remaining issues that failed the script). We can then use cases where the "matchMethod" is "NotMatched" to go back and see if we can find name matches. This is particularly going to be the case where we find more than one match on a fuzzy search, which I still haven't dealt with.
# 
# We also need to figure out what to do when we want to update the information over time. With ITIS, once we have a matched TSN, we can then use that TSN to grab updates as they occur, including changes in taxonomy. But we need to figure out if we should change the structure of the TIR cache to keep all the old versions of what we found over time so that it can always be referred back to.

# In[44]:

for feature in uniqueNames["features"]:
    # Set up a local data structure for storage and processing
    thisRecord = {}
    
    # Set data from query results
    thisRecord["gid"] = feature["properties"]["gid"]
    thisRecord["scientificname_submitted"] = feature["properties"]["scientificname"]
    thisRecord["scientificname_search"] = cleanScientificName(thisRecord["scientificname_submitted"])
    
    # Set defaults for thisRecord
    thisRecord["itisFoundFromFuzzy"] = 0
    thisRecord["matchMethod"] = "NotMatched:"+thisRecord["scientificname_search"]
    thisRecord["itisPairs"] = packageITISPairs(thisRecord["matchMethod"],0)

    # Handle the cases where there is enough interesting stuff in the scientific name string that it comes back blank from the cleaners
    if len(thisRecord["scientificname_search"]) != 0:
        # The ITIS Solr service does not fail in an elegant way, and so we need to try this whole section and except it out if the query fails
        try:
            thisRecord["itisSearchURL"] = getITISSearchURL(thisRecord["scientificname_search"])

            # Try an exact match search
            itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
            thisRecord["itisFoundFromExact"] = len(itisSearchResults["response"]["docs"])

            # Try a fuzzy match search and set match method
            if thisRecord["itisFoundFromExact"] == 0:
                itisSearchResults = requests.get(thisRecord["itisSearchURL"]+"~0.5").json()
                thisRecord["itisFoundFromFuzzy"] = len(itisSearchResults["response"]["docs"])
                if thisRecord["itisFoundFromFuzzy"] == 1:
                    thisRecord["matchMethod"] = "FuzzyMatch:"+thisRecord["scientificname_search"]
            elif thisRecord["itisFoundFromExact"] == 1:
                thisRecord["matchMethod"] = "ExactMatch:"+thisRecord["scientificname_search"]

            # If there are results from exact or fuzzy match search, package the ITIS properties we want
            if len(itisSearchResults["response"]["docs"]) == 1:
                thisRecord["itisPairs"] = packageITISPairs(thisRecord["matchMethod"],itisSearchResults)
            else:
                thisRecord["itisPairs"] = packageITISPairs(thisRecord["matchMethod"],0)
            
            # Handle cases where discovered TSN usage is invalid by following accepted TSN
            if (thisRecord["itisPairs"].find('"usage"=>"valid"') == -1 or thisRecord["itisPairs"].find('"usage"=>"accepted"') == -1) and thisRecord["itisPairs"].find('"acceptedTSN"') > 0:
                thisRecord["acceptedTSN"] = re.search('\"acceptedTSN"\=\>\"(.+?)\"', thisRecord["itisPairs"]).group(1)
                thisRecord["discoveredTSN"] = re.search('\"tsn"\=\>\"(.+?)\"', thisRecord["itisPairs"]).group(1)

                thisRecord["itisSearchURL"] = getITISSearchURL(thisRecord["acceptedTSN"])
                itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
                thisRecord["itisFoundFromAcceptedTSN"] = len(itisSearchResults["response"]["docs"])
                if thisRecord["itisFoundFromAcceptedTSN"] == 1:
                    thisRecord["matchMethod"] = "AcceptedTSNFollow:"+thisRecord["scientificname_search"]
                    thisRecord["itisPairs"] = packageITISPairs(thisRecord["matchMethod"],itisSearchResults)
                    thisRecord["itisPairs"] = thisRecord["itisPairs"]+',"discoveredTSN"=>"'+thisRecord["discoveredTSN"]+'"'
            
        except:
            pass
    
    display (thisRecord)
    print (cacheToTIR(thisRecord["gid"],"itis",thisRecord["itisPairs"].replace("'","''")))


# In[ ]:



