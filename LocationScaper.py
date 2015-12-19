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
import numpy as np
import scipy as sp

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
    parser.add_argument("--ms", help="Reload the mine sizes from earth engine",
                        action="store_true")
    parser.add_argument("--verbose", help="Verbose output",
                        action="store_true")
    args = parser.parse_args()

    #Get Mine location Data
    if not os.path.isfile(mineDataFile) or args.md:
        mineNames = fetchMineNames()
        mineList = []
        for mineName in mineNames:
            mineLocation, mineProducts = fetchMineData(mineName)
            if(mineLocation is not None and mineProducts):
                mineList.append({ 'location' : mineLocation, 'products' : mineProducts })
                if args.verbose:
                    print(mineName + " (lat: " + str(mineLocation[0]) + ", lon: " + str(mineLocation[1]) + "), which produces: " + ", ".join(mineProducts) + ".")
        #Save the data so it doesn't need loading next time
        fp = open(mineDataFile, 'w')
        json.dump(mineList, fp)
        fp.close()
    else:
        #Load the saved data
        fp = open(mineDataFile, 'r')
        mineList = json.load(fp)
        fp.close()

    #Get Mine Size Data from Earth Engine
    ee.Initialize()
    for mineData in mineList[0:1]:
        if args.ms or not 'size' in mineData:
            eviCollection = ee.ImageCollection("MODIS/MOD09GA_EVI")
            mineVegetation = mineChangeTime(eviCollection,
                                            mineData['location'][1], mineData['location'][0], mineDifference)
            controlVegetation = mineChangeTime(eviCollection,
                                               mineData['location'][1], mineData['location'][0], controlDifference)
            for mineSize, controlSize in zip(mineVegetation, controlVegetation):
                if(mineSize[1] is not None and controlSize[1] is not None):
                    seasonScaledSize = 1 - (mineSize[1] / (controlSize[1] - mineSize[1] + 0.1))
                    curDate = pandas.to_datetime(mineSize[0], unit='ms')
                    if args.verbose:
                        #Verbose output in csv format of mine size and prices at each timestep
                        printBase = str(mineSize[0]) + ", " + str(seasonScaledSize)
                        for commodity in mineData['products']:
                            cPrice = getCommodityPrice(commodity, curDate)
                            printBase = printBase + ", "+ str(cPrice)
                        print printBase
                    mineData['size'] = (seasonScaledSize, curDate)
        #save the mine data from earth engine for later
        fp = open(mineDataFile, 'w')
        json.dump(mineList, fp)
        fp.close()


    predictRange = 45
    for mineData in mineList[0:1]:
        RSquared = {}
        for commodity in mineData['products']:
            RSquared[commodity] = {}
            prices = []
            for mineSize, date in mineData['size']:
                prices.append(getCommodityPrice(commodity, date))
            #for td in pandas.to_timedelta(np.arange(-predictRange, predictRange), unit='d'):
            scipy.linregress()



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
