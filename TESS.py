
# coding: utf-8

# In[14]:

import requests,configparser,re
from IPython.display import display


# In[28]:

# Get API keys and any other config details from a file that is external to the code.
config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))


# In[16]:

# Build base URL with API key using input from the external config.
def getBaseURL():
    gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
    apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey
    return apiBaseURL


# In[35]:

# Basic function to insert subject ID, property, and value into tircache
def cacheToTIR(gid,infotype,pairs):
    updateQ = "UPDATE tir.tir2 SET "+infotype+" = '"+pairs+"' WHERE gid = "+str(gid)
    return requests.get(getBaseURL()+"&q="+updateQ).json()


# In[30]:

def queryTESS(tsn):
    tessSpeciesQueryByTSNBaseURL = "https://ecos.fws.gov/ecp0/TessQuery?request=query&xquery=/SPECIES_DETAIL[TSN="
    return requests.get(tessSpeciesQueryByTSNBaseURL+tsn+"]").text


# In[24]:

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


# In[33]:

tsns = requests.get(getBaseURL()+"&q=SELECT gid,itis->'discoveredTSN' AS discoveredtsn,itis->'acceptedTSN' AS acceptedtsn FROM tir.tir2 WHERE tess IS NULL AND itis->'itisMatchMethod' NOT LIKE 'NotMatched%'").json()


# In[ ]:

for feature in tsns["features"]:
    thisRecord = {}
    thisRecord["gid"] = feature["properties"]["gid"]
    thisRecord["discoveredTSN"] = feature["properties"]["discoveredtsn"]
    thisRecord["acceptedTSN"] = feature["properties"]["acceptedtsn"]
    
    # Query based on discovered TSN and package data
    thisRecord["tessPairs"] = packageTESSPairs(thisRecord["discoveredTSN"],queryTESS(thisRecord["discoveredTSN"]))

    if '"result"=>"none"' in thisRecord["tessPairs"] and type(thisRecord["acceptedTSN"]) is str:
        # Query based on discovered TSN and package data
        thisRecord["tessPairs"] = packageTESSPairs(thisRecord["acceptedTSN"],queryTESS(thisRecord["acceptedTSN"]))

    display (thisRecord)
    print (cacheToTIR(thisRecord["gid"],"tess",thisRecord["tessPairs"]))


# In[ ]:



