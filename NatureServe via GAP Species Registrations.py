
# coding: utf-8

# This notebook presents an alternate way of retrieving NatureServe information where the NatureServe ID associated with a taxa registry is already present in the registration information. Eventually, this needs to be put together with the previous NatureServe notebook that looks up the Element Global ID for a name and then runs the code to retrieve it.

# In[1]:

import requests,configparser,datetime,re
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
    


# In[5]:

# Basic function to insert subject ID, property, and value into tircache
def cacheToTIR(gid,infotype,pairs):
    updateQ = "UPDATE tir.tir2 SET "+infotype+" = '"+pairs+"' WHERE gid = "+str(gid)
    return requests.get(getBaseURL()+"&q="+updateQ).json()


# In[7]:

q_gapSpecies = "SELECT gid,registration->'EGTID' AS egtid    FROM tir.tir2     WHERE exist(registration, 'EGTID')     AND natureserve IS NULL"
r_gapSpecies = requests.get(getBaseURL()+"&q="+q_gapSpecies).json()

for feature in r_gapSpecies["features"]:
    print (cacheToTIR(feature["properties"]["gid"],"natureserve",packageNatureServePairs("ELEMENT_GLOBAL.2."+str(feature["properties"]["egtid"]))))



# In[ ]:



