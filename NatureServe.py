
# coding: utf-8

# This notebook uses taxon names from ITIS matches, looks for those names from the NatureServe species services, and returns key/value pairs for caching in the Taxonomic Information Registry and using in our systems.

# In[12]:

import requests,configparser,re
from IPython.display import display
from lxml import etree
from io import StringIO


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


# In[4]:

# Basic function to insert subject ID, property, and value into tircache
def cacheToTIR(gid,infotype,pairs):
    updateQ = "UPDATE tir.tir2 SET "+infotype+" = '"+pairs+"' WHERE gid = "+str(gid)
    return requests.get(getBaseURL()+"&q="+updateQ).json()


# ### Query to get the NatureServe Element Global ID
# NatureServe provides an open public service to query on a taxon name and get back their own specific ID called the Element Global ID. (There are also nation-specific IDs, but I'm not messing with those right now.) This function grabs that ID and sends it back.

# In[9]:

def queryNatureServeID(scientificname):
    natureServeSpeciesQueryBaseURL = "https://services.natureserve.org/idd/rest/v1/globalSpecies/list/nameSearch?name="
    natureServeData = requests.get(natureServeSpeciesQueryBaseURL+scientificname).text

    if natureServeData.find('<speciesSearchResultList>\r\n</speciesSearchResultList>') > 0:
        return "none"
    else:
        rawXML = natureServeData.replace('<?xml version="1.0" encoding="utf-8"?>\r\n\r\n', '')
        rawXML = rawXML.replace(' \r\n    xsi:schemaLocation="http://services.natureserve.org/docs/schemas/biodiversityDataFlow/1 http://services.natureserve.org/docs/schemas/biodiversityDataFlow/1/" \r\n    xmlns="http://services.natureserve.org/docs/schemas/biodiversityDataFlow/1" \r\n    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \r\n    schemaVersion="1.1"', '')
        f = StringIO(rawXML)
        tree = etree.parse(f)
        return tree.xpath('/speciesSearchReport/speciesSearchResultList/speciesSearchResult/globalSpeciesUid')[0].text


# ### Query for and package NatureServe key/value pairs
# This function uses the Element Global ID retrieved already and attempts to get the information we want to use in our systems. It will always return something as paired information. By default, it will return the date/time of the cache and a status value. Status can be "none" (for cases where we did not find an Element Global ID for the record being examined) or "error" (for cases where the search service comes back with no document). Otherwiase, it will return data in key/value pairs.

# In[1]:

def packageNatureServePairs(elementGlobalID):
    import datetime
    dt = datetime.datetime.utcnow().isoformat()
    natureServePairs = '"cacheDate"=>"'+dt+'"'
    natureServePairs = natureServePairs+',"elementGlobalID"=>"'+elementGlobalID+'"'
    
    if elementGlobalID == "none":
        natureServePairs = natureServePairs+',"status"=>"Not Found"'
        return natureServePairs
    else:
        baseURL_NatureServe = config.get('baseURLs', 'natureServeSpeciesQueryBaseURL').replace('"','')
        apiKey_NatureServe = config.get('apiKeys','apiKey_NatureServe').replace('"','')
        natureServeQueryURL = baseURL_NatureServe+"?NSAccessKeyId="+apiKey_NatureServe+"&uid="+elementGlobalID
    
        natureServeData = requests.get(natureServeQueryURL)

        strNatureServeData = natureServeData.text
        strNatureServeData = strNatureServeData.replace('<?xml version="1.0" encoding="utf-8"?>\r\n\r\n', '')
        strNatureServeData = strNatureServeData.replace('\r\n    xsi:schemaLocation="http://services.natureserve.org/docs/schemas/biodiversityDataFlow/1 http://services.natureserve.org/docs/schemas/biodiversityDataFlow/1/"\r\n    xmlns="http://services.natureserve.org/docs/schemas/biodiversityDataFlow/1"\r\n    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\r\n    schemaVersion="1.1"', '')
        f = StringIO(strNatureServeData)
        tree = etree.parse(f)
        root = tree.getroot()
        docLength = len(root.getchildren())

        # Test the response because I've found that not everything with a global element ID seems to come back with a response here
        if docLength > 0:
            # Grab out the specific elements we want to cache
            natureServePairs = natureServePairs+',"GlobalStatusRank"=>"'+tree.xpath('/globalSpeciesList/globalSpecies/conservationStatus/natureServeStatus/globalStatus/rank/code')[0].text+'"'
            natureServePairs = natureServePairs+',"roundedGlobalStatusRankDescription"=>"'+tree.xpath('/globalSpeciesList/globalSpecies/conservationStatus/natureServeStatus/globalStatus/roundedRank/description')[0].text+'"'
            try:
                natureServePairs = natureServePairs+',"globalStatusLastReviewed"=>"'+tree.xpath('/globalSpeciesList/globalSpecies/conservationStatus/natureServeStatus/globalStatus/statusLastReviewed')[0].text+'"'
            except:
                natureServePairs = natureServePairs+',"globalStatusLastReviewed"=>"Unknown"'

            try:
                natureServePairs = natureServePairs+',"usNationalStatusRankCode"=>"'+tree.xpath("//nationalStatus[@nationCode='US']/rank/code")[0].text+'"'
            except:
                pass
            try:
                natureServePairs = natureServePairs+',"usNationalStatusLastReviewed"=>"'+tree.xpath("//nationalStatus[@nationCode='US']/statusLastReviewed")[0].text+'"'
            except:
                natureServePairs = natureServePairs+',"usNationalStatusLastReviewed"=>"Unknown"'

            try:
                # Loop through US states in the "subnationalStatuses" and put state names and codes into the tircache
                usStatesTree = etree.ElementTree(tree.xpath("//nationalStatus[@nationCode='US']/subnationalStatuses")[0])
                for elem in usStatesTree.iter():
                    stateName = elem.attrib.get('subnationName')
                    if isinstance(stateName, str):
                        natureServePairs = natureServePairs+',"StateCode:'+stateName+'"=>"'+tree.xpath("//subnationalStatus[@subnationName='"+stateName+"']/rank/code")[0].text+'"'
            except:
                pass
        else:
            natureServePairs = natureServePairs+',"status"=>"error"'

        return natureServePairs
    


# ### Get data to process
# Right now, we run this against ITIS names where the NatureServe information is null. In future, we need to work out how we run this periodically to refresh the cache and what we do with the previous data returned from the service.

# In[17]:

itisNames = requests.get(getBaseURL()+"&q=SELECT gid,itis->'nameWInd' AS namewind,itis->'nameWOInd' AS namewoind FROM tir.tir2 WHERE natureserve IS NULL AND itis->'itisMatchMethod' NOT LIKE 'NotMatched%'").json()


# ### Loop through the names and run the functions
# We set up a local record to house everything, run the functions, and update results in the TIR. If necessary and applicable, we try both the nameWOInd and nameWInd values from ITIS.

# In[21]:

for feature in itisNames["features"]:
    thisRecord = {}
    thisRecord["gid"] = feature["properties"]["gid"]
    thisRecord["nameWOInd"] = feature["properties"]["namewoind"]
    thisRecord["nameWInd"] = feature["properties"]["namewind"]
    
    # Try to find a NatureServe ID with nameWOInd
    thisRecord["elementGlobalID"] = queryNatureServeID(thisRecord["nameWOInd"])
    
    # Try to find a NatureServe ID with nameWInd if possible
    if thisRecord["elementGlobalID"] == "none" and thisRecord["nameWInd"] != thisRecord["nameWOInd"]:
        thisRecord["elementGlobalID"] = queryNatureServeID(thisRecord["nameWInd"])

    # Run the function to query and pacage NatureServe key/value pairs
    thisRecord["natureServePairs"] = packageNatureServePairs(thisRecord["elementGlobalID"])

    # Display the record, cache results, and show query status
    display (thisRecord)
    print (cacheToTIR(thisRecord["gid"],"natureserve",thisRecord["natureServePairs"]))


# In[ ]:



