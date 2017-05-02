
# coding: utf-8

# This notebook retrieves registered species from a new attempt at what the Taxonomic Information Registry might look like, processes the species names through a little bit of logic, checks them against the ITIS Solr service, pairs up the properties we want to cache, and then commits them back to the "tir2" data table. It uses a number of common functions in the tirutils.py package that I'm working with.
# 
# Right now, this is explicitly grabbing SGCN submitted scientific names, but it can be tweaked to retrieve any taxon string and do a lookup. We'll need to fiddle with the data from ITIS for our applications, but here's what I'm grabbing so far:
# 
# * discovered and accepted TSN
# * taxonomic hierarchy (taking advantage of the nice way the Solr index includes the valid taxonomy without having to track down the accepted TSN record itself)
# * vernacular names (different languages)
# * rank of the discovered taxon, so that we can determine whether we only want to display cases where species or better is identified
# * create and update dates so we know how current the ITIS records are

# In[13]:

import tirutils,requests,configparser,re
from IPython.display import display

config = configparser.RawConfigParser()
config.read_file(open(r'../config/stuff.py'))
gc2APIKey = config.get('apiKeys','apiKey_GC2_BCB').replace('"','')
apiBaseURL = "https://gc2.mapcentia.com/api/v1/sql/bcb?key="+gc2APIKey


# In[14]:

# Retrieve target data from the TIR table where we do not yet have ITIS matched and cached
targetDataSQL = "SELECT gid,     registration -> 'SGCN_ScientificName_Submitted' AS scientificname     FROM tir.tir2     WHERE itis IS NULL     ORDER BY gid"

targetData = requests.get(apiBaseURL+"&q="+targetDataSQL).json()


# In[15]:

# Set this flag to false to show the results but not commit data
commitData = True

for feature in targetData['features']:
    gid = feature['properties']['gid']
    scientificname = feature['properties']['scientificname']
    itisTerm = "nameWOInd"
    numFoundExact = 0
    numFoundFuzzy = 0

    # "var." and "ssp." indicate that the string has population and variation indicators and should use the WInd service
    if scientificname.find("var.") or scientificname.find("ssp."):
        itisTerm = "nameWInd"

    # Get rid of "pop." from the string to enable the search to find a match without whatever population indicator is in the string
    if scientificname.find("pop."):
        nameParts = scientificname.split()
        regex = re.compile(r'pop\.[0-9]')
        scientificname = ' '.join([i for i in nameParts if not regex.search(i)])

    # Get rid of "spp." from the string, and it should find a genus match
    if scientificname.find("spp."):
        nameParts = scientificname.split()
        regex = re.compile(r'spp\.')
        scientificname = ' '.join([i for i in nameParts if not regex.search(i)])

# Revisit this to cut out the actual stuff between the ()
#    if scientificname.find("(") and scientificname.find(")"):
#        nameParts = scientificname.split()
#        regex = re.compile(r'\(')
#        scientificname = ' '.join([i for i in nameParts if not regex.search(i)])

    # Clean up all upper case strings because the ITIS service doesn't like them
    if any(x.isupper() for x in scientificname[-(len(scientificname)-1):]):
        scientificname = scientificname.lower().capitalize()

    # First try an exact match on the name string
    itisExactMatchURL = "http://services.itis.gov/?wt=json&rows=10&q="+itisTerm+":"+scientificname.replace(" ","\%20")
    try:
        itisExactMatchR = requests.get(itisExactMatchURL).json()
        numFoundExact = int(itisExactMatchR['response']['numFound'])
        if numFoundExact == 1:
            itisPairs = tirutils.packageITISPairs('exact match on '+scientificname,itisExactMatchR)
            print ("Found an exact match for "+scientificname+", so going to the next item")
            if commitData:
                print (tirutils.cacheToTIR(apiBaseURL,gid,"itis",itisPairs))
            continue
        elif numFoundExact > 1:
            continue
    except:
        print ("Problem with exact match: "+itisExactMatchURL)
        continue

    # If an exact match doesn't work or has found more than one record, try a fuzzy match search
    itisFuzzyMatchURL = "http://services.itis.gov/?wt=json&rows=10&q="+itisTerm+":"+scientificname.replace(" ","\%20")+"~0.5"
    try:
        itisFuzzyMatchR = requests.get(itisFuzzyMatchURL).json()
        numFoundFuzzy = int(itisFuzzyMatchR['response']['numFound'])
        if numFoundFuzzy == 1:
            itisPairs = tirutils.packageITISPairs('fuzzy match on '+scientificname,itisFuzzyMatchR)
            print ("Found a fuzzy match for "+scientificname+", so going to the next item")
            if commitData:
                print (tirutils.cacheToTIR(apiBaseURL,gid,"itis",itisPairs))
            continue
        elif numFoundFuzzy > 1:
            continue
    except:
        print ("Problem with fuzzy match: "+itisFuzzyMatchURL)
        continue

    # Show an output indicating that the name string will still need some work and indicating cases where more than one match was found
    print ("Still need to work on: "+scientificname+" ("+str(numFoundExact)+","+str(numFoundFuzzy)+")")
    


# In[ ]:



