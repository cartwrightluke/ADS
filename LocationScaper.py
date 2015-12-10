import urllib.request
import urllib.parse
import os.path
import json
import re
import argparse

wikipediaBase = "https://en.wikipedia.org/w/api.php?action=query&prop=revisions|coordinates&rvprop=content&format=json&redirects=&titles="
wikiUAHeader = { "User-Agent" : "(Mining research data collector, for problems contact luke@lukecartwright.com)" }
mineDataFile = 'mineData.dump'
mapDifference = 0.05

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--md", help="Reload the mine data from wikipedia",
                        action="store_true")
    args = parser.parse_args()

    if not os.path.isfile(mineDataFile) or args.md:
        mineNames = fetchMineNames()
        mineData = []
        for mineName in mineNames:
            mineLocation, mineProducts = fetchMineData(mineName)
            if(mineLocation is not None and mineProducts):
                mineData.append((mineLocation, mineProducts))
                #print(mineName + " (lat: " + str(mineLocation[0]) + ", lon: " + str(mineLocation[1]) + "), which produces: " + ", ".join(mineProducts) + ".")
        fp = open(mineDataFile, 'w')
        json.dump(mineData, fp)
        fp.close()
    else:
        fp = open(mineDataFile, 'r')
        mineData = json.load(fp)
        fp.close()

    print(mineData)

def fetchMineNames():
    page, _ = wikipediaRequest("List_of_open-pit_mines")
    r = re.compile("^\*[^\[\]]*?\[\[(?P<mineName>[^\[\]\|]+)", re.MULTILINE)
    return [m.groupdict()["mineName"] for m in r.finditer(page)]

def wikipediaRequest(article):
    requestUrl = wikipediaBase + urllib.parse.quote(str(article), '-')
    req = urllib.request.Request(requestUrl, headers = wikiUAHeader, method='GET')
    with urllib.request.urlopen(req) as url:
        queryResponse = json.loads(url.read().decode('utf-8'))['query']
        pageData = list(queryResponse['pages'].values())[0]
        if 'missing' in pageData:
            return None, None
        elif 'normalized' in queryResponse:
            return wikipediaRequest(queryResponse['normalized'][0]['to'])
        else:
            if 'coordinates' in pageData:
                location = (pageData['coordinates'][0]['lat'], pageData['coordinates'][0]['lon'])
            else:
                location = None
            return pageData['revisions'][0]['*'], location

def fetchMineData(mineName):
    page, location = wikipediaRequest(mineName)
    if(page is None):
        return None, None

    #search the page for material information
    possibleMaterials = {'Gold' : 'Gold', 'Silver' : 'Silver', 'Copper' : 'Copper|Malachite', 'Uranium' : 'Uranium',
                         'Coal' : 'Coal|Lignite', 'Diamond' : 'Diamond|Kimberlite', 'Lead' : 'Lead|Galena'}
    materials = []
    for materialName, materialRegex in possibleMaterials.items():
        found = re.search(materialRegex, page, re.IGNORECASE);
        if found is not None:
            materials.append(materialName)

    return location, materials



if __name__ == "__main__":
    main()
