
# coding: utf-8

# This notebook runs through submitted species names, looks for matches in the World Register of Marine Species, and processes out specific properties that we want associated with discovered taxa in the Taxonomic Information Registry. 

# In[36]:

import tirutils,requests,configparser,re
from IPython.display import display

config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))
gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey

wormsRecordsByNameBaseURL = "http://www.marinespecies.org/rest/AphiaRecordsByName/"
wormsRecordByAphiaIDBaseURL = "http://www.marinespecies.org/rest/AphiaRecordByAphiaID/"


# In[42]:

# Pair worms properties that we want to cache
def packageWoRMSPairs(matchMethod,wormsData):
    import datetime
    dt = datetime.datetime.utcnow().isoformat()
    wormsPairs = '"cacheDate"=>"'+dt+'"'
    wormsPairs = wormsPairs+',"AphiaID"=>"'+str(wormsData['AphiaID'])+'"'
    wormsPairs = wormsPairs+',"wormsMatchMethod"=>"'+matchMethod+'"'
    wormsPairs = wormsPairs+',"scientificname"=>"'+wormsData['scientificname']+'"'
    wormsPairs = wormsPairs+',"status"=>"'+wormsData['status']+'"'
    wormsPairs = wormsPairs+',"rank"=>"'+wormsData['rank']+'"'
    wormsPairs = wormsPairs+',"valid_name"=>"'+wormsData['valid_name']+'"'
    try:
        wormsPairs = wormsPairs+',"valid_AphiaID"=>"'+str(wormsData['valid_AphiaID'])+'"'
    except:
        pass
    wormsPairs = wormsPairs+',"kingdom"=>"'+wormsData['kingdom']+'"'
    wormsPairs = wormsPairs+',"phylum"=>"'+wormsData['phylum']+'"'
    wormsPairs = wormsPairs+',"class"=>"'+wormsData['class']+'"'
    wormsPairs = wormsPairs+',"order"=>"'+wormsData['order']+'"'
    wormsPairs = wormsPairs+',"family"=>"'+wormsData['family']+'"'
    wormsPairs = wormsPairs+',"genus"=>"'+wormsData['genus']+'"'
    wormsPairs = wormsPairs+',"lsid"=>"'+wormsData['lsid']+'"'
    wormsPairs = wormsPairs+',"isMarine"=>"'+str(wormsData['isMarine'])+'"'
    wormsPairs = wormsPairs+',"isBrackish"=>"'+str(wormsData['isBrackish'])+'"'
    wormsPairs = wormsPairs+',"isFreshwater"=>"'+str(wormsData['isFreshwater'])+'"'
    wormsPairs = wormsPairs+',"isTerrestrial"=>"'+str(wormsData['isTerrestrial'])+'"'
    wormsPairs = wormsPairs+',"isExtinct"=>"'+str(wormsData['isExtinct'])+'"'
    wormsPairs = wormsPairs+',"match_type"=>"'+wormsData['match_type']+'"'
    wormsPairs = wormsPairs+',"modified"=>"'+wormsData['modified']+'"'

    return wormsPairs


# In[43]:

# Get both the registered scientific name (SGCN only at this point) and any species name from ITIS so that we can run a couple of options for WoRMS matches
targetDataSQL = "SELECT gid,     registration -> 'SGCN_ScientificName_Submitted' AS scientificname,     itis -> 'Species' AS speciesname_itis     FROM tir.tir2     WHERE worms IS NULL     ORDER BY gid"

targetData = requests.get(apiBaseURL+"&q="+targetDataSQL).json()


# In[45]:

# Set this flag to true to go ahead and write data to the database
commitData = True

for feature in targetData['features']:
    gid = feature['properties']['gid']
    scientificname = feature['properties']['scientificname']
    speciesname_itis = feature['properties']['speciesname_itis']
    numFoundExact = 0
    numFoundFuzzy = 0

    if scientificname.find("pop."):
        nameParts = scientificname.split()
        regex = re.compile(r'pop\.[0-9]')
        scientificname = ' '.join([i for i in nameParts if not regex.search(i)])
    
    if scientificname.find("spp."):
        nameParts = scientificname.split()
        regex = re.compile(r'spp\.')
        scientificname = ' '.join([i for i in nameParts if not regex.search(i)])
    
    if any(x.isupper() for x in scientificname[-(len(scientificname)-1):]):
        scientificname = scientificname.lower().capitalize()
    
    wormsFuzzyMatchURL = wormsRecordsByNameBaseURL+scientificname+"?like=true&marine_only=false"
    worms = str()
    try:
        wormsFuzzyMatchR = requests.get(wormsFuzzyMatchURL).json()
    except:
        continue
    if len(wormsFuzzyMatchR) == 1:
        if wormsFuzzyMatchR[0]['AphiaID'] == wormsFuzzyMatchR[0]['valid_AphiaID']:
            wormsMatchMethod = "found match on scientific name"
            worms = packageWoRMSPairs(wormsMatchMethod,wormsFuzzyMatchR[0])
        else:
            aphiaIDURL = wormsRecordByAphiaIDBaseURL+str(wormsFuzzyMatchR[0]['valid_AphiaID'])
            try:
                aphiaR = requests.get(aphiaIDURL).json()
            except:
                continue
            wormsMatchMethod = "found match by following valid aphiaid"
            worms = packageWoRMSPairs(wormsMatchMethod,aphiaR)
    
    # If we failed to get anything on the originally submitted name, try the name from ITIS (if available)
    if len(worms) == 0 and type(speciesname_itis) != None and speciesname_itis != scientificname:
        wormsFuzzyMatchURL = wormsRecordsByNameBaseURL+speciesname_itis+"?like=true&marine_only=false"
        try:
            wormsFuzzyMatchR = requests.get(wormsFuzzyMatchURL).json()
        except:
            continue
        if len(wormsFuzzyMatchR) == 1:
            if wormsFuzzyMatchR[0]['AphiaID'] == wormsFuzzyMatchR[0]['valid_AphiaID']:
                wormsMatchMethod = "found match on ITIS name"
                worms = packageWoRMSPairs(wormsMatchMethod,str(wormsFuzzyMatchR[0]))
            else:
                aphiaIDURL = wormsRecordByAphiaIDBaseURL+str(wormsFuzzyMatchR[0]['valid_AphiaID'])
                try:
                    aphiaR = requests.get(aphiaIDURL).json()
                except:
                    continue
                wormsMatchMethod = "found match by following valid AphiaID from ITIS name"
                worms = packageWoRMSPairs(wormsMatchMethod,aphiaR)

    if len(worms) > 0 and commitData:
        print (tirutils.cacheToTIR(apiBaseURL,gid,"worms",worms))
    elif len(worms) > 0 and not commitData:
        print (gid, worms)


# In[ ]:



