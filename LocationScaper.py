import urllib2
import os.path
import json
import re
import argparse
import ee
import Quandl
import datetime
import calendar
import pandas

wikipediaBase = "https://en.wikipedia.org/w/api.php?action=query&prop=revisions|coordinates&rvprop=content&format=json&redirects=&titles="
wikiUAHeader = { "User-Agent" : "(Mining research data collector, for problems contact luke@lukecartwright.com)" }
mineDataFile = 'mineData.dump'
mineDifference = 0.025
controlDifference = 0.05
startDate = '2008-01-01'
#startDate = '2015-05-01'
endDate = '2015-09-01'
quandlKey = 'djkeQSyVhCPzKsxYHXtd'

commodityPrices = {}

def getCommodityPrice(type, date):
    commodityTypes = { 'Gold' : 'PERTH/GOLD_USD_D.1', 'Silver' : 'PERTH/SLVR_USD_D.1', 'Coal' : 'ODA/PCOALAU_USD',
                       'Uranium' : 'ODA/PURAN_USD', 'Lead' : 'ODA/PLEAD_USD', 'Copper' : 'ODA/PCOPP_USD'}

    if type not in commodityTypes :
        raise NameError(str(type) + ' is not a valid commodity.')

    global commodityPrices
    if(type not in commodityPrices):
        startDateInclusive = str(pandas.Timestamp(startDate) + pandas.Timedelta(days=-31))
        endDateInclusive = str(pandas.Timestamp(endDate) + pandas.Timedelta(days=31))
        data = Quandl.get(commodityTypes[type], returns='numpy', authtoken= quandlKey, trim_start=startDateInclusive,
                          trim_end=endDateInclusive, exclude_column_names = True)
        lastIndex = 0
        curIndex = 0
        commodityPrices[type] = {}
        for curDate in pandas.date_range(startDate, endDate):
            last = data[lastIndex]
            cur = data[curIndex]
            while(curDate > cur[0]):
                lastIndex = curIndex
                curIndex = curIndex + 1
                last = data[lastIndex]
                cur = data[curIndex]
            if(cur[0] == curDate or cur[0] == curDate):
                commodityPrices[type][curDate] = cur[1]
                lastIndex = curIndex
                curIndex = curIndex + 1
            else :
                interpolated = last[1] + (cur[1] - last[1]) * (float((curDate - last[0]).days) / float((cur[0] - last[0]).days))
                commodityPrices[type][curDate] = interpolated

    if isinstance(date, str):
        date = pandas.Timestamp(date)
    elif not isinstance(date, pandas.Timestamp):
        raise TypeError('Unknown date type')

    if date not in commodityPrices[type]:
        raise ValueError('Given date not within the valid range')

    return commodityPrices[type][date]



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--md", help="Reload the mine data from wikipedia",
                        action="store_true")
    args = parser.parse_args()

    #Get Mine Data
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

    #Get Mine Data from Earth Engine
    for mine in mineData[0:1]:
        ee.Initialize()
        eviCollection = ee.ImageCollection("MODIS/MCD43A4_EVI")
        mineVegetation = mineChangeTime(eviCollection, mine[0][1], mine[0][0], mineDifference)
        controlVegetation = mineChangeTime(eviCollection, mine[0][1], mine[0][0], controlDifference)
        for mine, control in zip(mineVegetation, controlVegetation):
            if(mine[1] is not None and control[1] is not None):
                result = 1 - (mine[1] / (control[1] - mine[1] + 0.1))
                curDate = pandas.to_datetime(mine[0], unit='ms')
                priceg = getCommodityPrice('Gold', curDate)
                prices = getCommodityPrice('Silver', curDate)
                print str(mine[0]) + ", "+ str(result) + ", "+ str(priceg) + ", "+ str(prices)

    print(mineData)

def fetchMineNames():
    page, _ = wikipediaRequest("List_of_open-pit_mines")
    r = re.compile("^\*[^\[\]]*?\[\[(?P<mineName>[^\[\]\|]+)", re.MULTILINE)
    return [m.groupdict()["mineName"] for m in r.finditer(page)]

def wikipediaRequest(article):
    requestUrl = wikipediaBase + urllib2.quote(str(article), '-')
    req = urllib2.Request(requestUrl, headers = wikiUAHeader, method='GET')
    with urllib2.urlopen(req) as url:
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
                         'Coal' : 'Coal|Lignite', 'Lead' : 'Lead|Galena|Zinc|Tin'}
    materials = []
    for materialName, materialRegex in possibleMaterials.items():
        found = re.search(materialRegex, page, re.IGNORECASE);
        if found is not None:
            materials.append(materialName)

    return location, materials

def mineChangeTime(eviCollection, lon, lat, area):
    SCALE = 500
    region = ee.Geometry.Rectangle(ee.Number(lon - area), ee.Number(lat - area), ee.Number(lon + area), ee.Number(lat + area))

    def getMean(image, list):
        mineSize = image.reduceRegion( ee.Reducer.mean(), region, crs='EPSG:4326', scale=SCALE, maxPixels = 1e9)
        avgTime = ee.Number(image.get("system:time_start")).add(ee.Number(image.get('system:time_end'))).divide(2)
        curFeat = ee.Feature(None, {'time': avgTime, 'EVI': mineSize})
        return ee.List(list).add(curFeat)

    features = ee.FeatureCollection(eviCollection.filterDate(startDate, endDate).iterate(getMean, ee.List([])))
    return map(lambda x: (x['properties']['time'], x['properties']['EVI']['EVI']), features.getInfo())


if __name__ == "__main__":
    main()
