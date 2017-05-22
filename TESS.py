
# coding: utf-8

# This notebook used ITIS TSNs discovered and cached in the Taxonomic Information Registry to look for information from the USFWS Threatened and Endangered Species System web service. It cached either a negative result or a set of key/value pairs from the TESS service of interest in characterizing species in the TIR.

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


# In[4]:

# Basic function to insert subject ID, property, and value into tircache
def cacheToTIR(gid,infotype,pairs):
    updateQ = "UPDATE tir.tir2 SET "+infotype+" = '"+pairs+"' WHERE gid = "+str(gid)
    return requests.get(getBaseURL()+"&q="+updateQ).json()


# ### Query TESS by TSN
# There are several potential query points into TESS, but for now, we are just using the TSN-based search.

# In[5]:

def queryTESS(tsn):
    tessSpeciesQueryByTSNBaseURL = "https://ecos.fws.gov/ecp0/TessQuery?request=query&xquery=/SPECIES_DETAIL[TSN="
    return requests.get(tessSpeciesQueryByTSNBaseURL+tsn+"]").text


# ### Package up the TESS data
# This function works through the XML result from the TESS service. It will always return the data/time that the packaging step ran, the TSN that was used, and a result code. The result can be "none", "error" or "success". If successful, information is packaged from specific TESS attributes of interest into key/value pairs for later use.

# In[6]:

def packageTESSPairs(tsn,tessData):
    import datetime
    from lxml import etree
    from io import StringIO
    dt = datetime.datetime.utcnow().isoformat()
    tessPairs = '"cacheDate"=>"'+dt+'"'
    tessPairs = tessPairs+',"tsn"=>"'+tsn+'"'

    if tessData.find('<results/>') > 0:
        tessPairs = tessPairs+',"result"=>"none"'
    else:
        try:
            rawXML = tessData.replace('<?xml version="1.0" encoding="iso-8859-1"?>', '')
            f = StringIO(rawXML)
            tree = etree.parse(f)
            tessPairs = tessPairs+',"result"=>"success"'
            tessPairs = tessPairs+',"entityId"=>"'+tree.xpath('/results/SPECIES_DETAIL/ENTITY_ID')[0].text+'"'
            tessPairs = tessPairs+',"SpeciesCode"=>"'+tree.xpath('/results/SPECIES_DETAIL/SPCODE')[0].text+'"'
            tessPairs = tessPairs+',"CommonName"=>"'+tree.xpath('/results/SPECIES_DETAIL/COMNAME')[0].text+'"'
            tessPairs = tessPairs+',"PopulationDescription"=>"'+tree.xpath('/results/SPECIES_DETAIL/POP_DESC')[0].text+'"'
            tessPairs = tessPairs+',"Status"=>"'+tree.xpath('/results/SPECIES_DETAIL/STATUS')[0].text+'"'
            tessPairs = tessPairs+',"StatusText"=>"'+tree.xpath('/results/SPECIES_DETAIL/STATUS_TEXT')[0].text+'"'
            rListingDate = tree.xpath('/results/SPECIES_DETAIL/LISTING_DATE')
            if len(rListingDate) > 0:
                tessPairs = tessPairs+',"ListingDate"=>"'+rListingDate[0].text+'"'
            tessPairs = tessPairs.replace("\'","''").replace(";","|").replace("--","-")
        except:
            tessPairs = tessPairs+',"result"=>"error"'

    return tessPairs


# ### Get TSNs for processing
# Right now, we retrieve everything from the TIR table that has an ITIS TSN and does not currently have any TESS data. In future, we should come up with some other way of triggering this processing service and come up with a plan for periodic checks and updates.

# In[7]:

tsns = requests.get(getBaseURL()+"&q=SELECT gid,itis->'tsn' AS tsn,itis->'acceptedTSN' AS acceptedtsn FROM tir.tir2 WHERE tess IS NULL AND itis->'itisMatchMethod' NOT LIKE 'NotMatched%'").json()


# ### Process the records to query and return information from TESS
# In this current process, I first check the discovered TSN from the ITIS cache. If that one does not return any results, I check the accepted TSN from that record (if it exists).

# In[8]:

for feature in tsns["features"]:
    thisRecord = {}
    thisRecord["gid"] = feature["properties"]["gid"]
    thisRecord["tsn"] = feature["properties"]["tsn"]
    thisRecord["acceptedTSN"] = feature["properties"]["acceptedtsn"]
    
    # Query based on discovered TSN and package data
    thisRecord["tessPairs"] = packageTESSPairs(thisRecord["tsn"],queryTESS(thisRecord["tsn"]))

    if '"result"=>"none"' in thisRecord["tessPairs"] and type(thisRecord["acceptedTSN"]) is str:
        # Query based on discovered TSN and package data
        thisRecord["tessPairs"] = packageTESSPairs(thisRecord["acceptedTSN"],queryTESS(thisRecord["acceptedTSN"]))

    display (thisRecord)
    print (cacheToTIR(thisRecord["gid"],"tess",thisRecord["tessPairs"]))


# In[ ]:



