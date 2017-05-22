
# coding: utf-8

# This notebook works with the Taxonomic Information Registry. It handles registrations where there is an explicit ITIS TSN identified for a taxon.

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


# ### Package up the specific attributes we want to cache from ITIS
# This function takes the data coming from the ITIS service as JSON and pairs up the attributes and values we want to cache and use. The date/time stamp here for when the information is cached is vital metadata for determining usability. As soon as the information comes out of ITIS, it is potentially stale. The information we collect and use from ITIS through this process includes the following:
# * Discovered and accepted TSNs for the taxon
# * Taxonomic rank of the discovered taxon
# * Names with and without indicators for the discovered taxon
# * Taxonomic hierarchy with ranks (in the ITIS Solr service, this is always the accepted taxonomic hierarchy)
# * Vernacular names for the discovered taxon

# In[4]:

def packageITISPairs(matchMethod,itisDoc):
    import datetime
    dt = datetime.datetime.utcnow().isoformat()
    itisPairs = '"cacheDate"=>"'+dt+'"'
    itisPairs = itisPairs+',"itisMatchMethod"=>"'+matchMethod+'"'

    if type(itisDoc) is int:
        return itisPairs
    else:
        itisPairs = itisPairs+',"createDate"=>"'+itisDoc['createDate']+'"'
        itisPairs = itisPairs+',"updateDate"=>"'+itisDoc['updateDate']+'"'
        itisPairs = itisPairs+',"tsn"=>"'+itisDoc['tsn']+'"'
        itisPairs = itisPairs+',"rank"=>"'+itisDoc['rank']+'"'
        itisPairs = itisPairs+',"nameWInd"=>"'+itisDoc['nameWInd']+'"'
        itisPairs = itisPairs+',"nameWOInd"=>"'+itisDoc['nameWOInd']+'"'
        itisPairs = itisPairs+',"usage"=>"'+itisDoc['usage']+'"'

        if 'acceptedTSN' in itisDoc:
            itisPairs = itisPairs+',"acceptedTSN"=>"'+itisDoc['acceptedTSN'][0]+'"'

        hierarchy = itisDoc['hierarchySoFarWRanks'][0]
        hierarchy = hierarchy[hierarchy.find(':$')+2:-1]
        hierarchy = '"'+hierarchy.replace(':', '"=>"').replace('$', '","')+'"'
        itisPairs = itisPairs+','+hierarchy

        if "vernacular" in itisDoc:
            vernacularList = []
            for commonName in itisDoc['vernacular']:
                commonNameElements = commonName.split('$')
                vernacularList.append('"vernacular:'+commonNameElements[2]+'"=>"'+commonNameElements[1]+'"')
            strVernacularList = ''.join(vernacularList).replace("\'", "''").replace('""','","')
            itisPairs = itisPairs+','+strVernacularList

        return itisPairs


# In[5]:

# Basic function to insert subject ID, property, and value into tircache
def cacheToTIR(gid,infotype,pairs):
    import requests
    updateQ = "UPDATE tir.tir2 SET "+infotype+" = '"+pairs+"' WHERE gid = "+str(gid)
    r = requests.get(getBaseURL()+"&q="+updateQ).json()
    return r


# In[26]:

# Query for the registered names we want to run through the system
tsns  = requests.get(getBaseURL()+"&q=SELECT gid,registration->'ITIS_TSN' AS tsn,registration->'GAP_SpeciesCode' AS gapcode FROM tir.tir2 WHERE itis IS NULL").json()


# ### Run the process for all supplied TSNs
# This is the process that should eventually be the substance of a microservice on ITIS info retrieval.

# In[27]:

for feature in tsns["features"]:
    # Set up a local data structure for storage and processing
    thisRecord = {}
    
    # Set data from query results
    thisRecord["gid"] = feature["properties"]["gid"]
    thisRecord["tsn"] = feature["properties"]["tsn"]
    thisRecord["GAP_SpeciesCode"] = feature["properties"]["gapcode"]
    thisRecord["itisSearchURL"] = "http://services.itis.gov/?wt=json&q=tsn:"+str(thisRecord["tsn"])
    
    if thisRecord["GAP_SpeciesCode"] is not None and thisRecord["tsn"] is not None:
        itisSearchResults = requests.get(thisRecord["itisSearchURL"]).json()
        thisRecord["numResults"] = len(itisSearchResults["response"]["docs"])
        
        if thisRecord["numResults"] == 1:
            thisRecord["matchMethod"] = "TSNRetrival:"+str(thisRecord["tsn"])
            thisRecord["itisPairs"] = packageITISPairs(thisRecord["matchMethod"],itisSearchResults["response"]["docs"][0])
            print (cacheToTIR(thisRecord["gid"],"itis",thisRecord["itisPairs"].replace("'","''")))
        else:
            print ("Something went wrong")

        display (thisRecord)


# In[ ]:



