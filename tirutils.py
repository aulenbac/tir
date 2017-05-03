#!/Users/sky/anaconda/bin/python
# Filename: tirutils.py

# Set the current time for use
def dt():
    import datetime
    return(datetime.datetime.utcnow().isoformat())

# Basic function to insert subject ID, property, and value into tircache
def cacheToTIR(apiBaseURL,gid,infotype,pairs):
    import requests
    updateQ = "UPDATE tir.tir2 SET "+infotype+" = '"+pairs+"' WHERE gid = "+str(gid)
    r = requests.get(apiBaseURL+"&q="+updateQ).json()
    return r

def packageITISPairs(matchMethod,itisJSON):
    import datetime
    dt = datetime.datetime.utcnow().isoformat()
    itisPairs = '"cacheDate"=>"'+dt+'"'
    itisPairs = itisPairs+',"itisMatchMethod"=>"'+matchMethod+'"'
    itisPairs = itisPairs+',"createDate"=>"'+itisJSON['response']['docs'][0]['createDate']+'"'
    itisPairs = itisPairs+',"updateDate"=>"'+itisJSON['response']['docs'][0]['updateDate']+'"'
    itisPairs = itisPairs+',"discoveredTSN"=>"'+itisJSON['response']['docs'][0]['tsn']+'"'
    itisPairs = itisPairs+',"rank"=>"'+itisJSON['response']['docs'][0]['rank']+'"'

    if 'acceptedTSN' in itisJSON['response']['docs'][0]:
        itisPairs = itisPairs+',"acceptedTSN"=>"'+''.join(str(e) for e in itisJSON['response']['docs'][0]['acceptedTSN'])+'"'

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

def packageTESSPairs(tessData):
    import datetime
    from lxml import etree
    from io import StringIO
    dt = datetime.datetime.utcnow().isoformat()
    try:
        rawXML = tessData.replace('<?xml version="1.0" encoding="iso-8859-1"?>', '')
        f = StringIO(rawXML)
        tree = etree.parse(f)
        tessPairs = '"cacheDate"=>"'+dt+'"'
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
        return tessPairs
    except:
        return "error"

def packageWoRMSPairs(matchMethod,wormsData):
    import datetime
    dt = datetime.datetime.utcnow().isoformat()
    wormsPairs = '"cacheDate"=>"'+dt+'"'
    wormsPairs = wormsPairs+',"itisMatchMethod"=>"'+matchMethod+'"'
    wormsPairs = wormsPairs+',"scientificname"=>"'+wormsData['scientificname']+'"'

    return wormsPairs
    

version = '0.1'

# End of tirutils.py