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
import scipy.stats
import matplotlib.pyplot as plt
import matplotlib.animation as animation

wikipediaBase = "https://en.wikipedia.org/w/api.php?action=query&prop=revisions|coordinates&rvprop=content&format=json&redirects=&titles="
wikiUAHeader = { "User-Agent" : "(Mining research data collector, for problems contact luke@lukecartwright.com)" }

quandlKey = 'djkeQSyVhCPzKsxYHXtd'

commodityTypes = { 'Gold' : 'PERTH/GOLD_USD_D.1', 'Silver' : 'PERTH/SLVR_USD_D.1', 'Coal' : 'ODA/PCOALAU_USD',
                       'Uranium' : 'ODA/PURAN_USD', 'Lead' : 'ODA/PLEAD_USD', 'Copper' : 'ODA/PCOPP_USD'}
startDate = None
endDate = None
def main():
    #Parse arguments
    parser = argparse.ArgumentParser(prog='EnvironmentalPlan.py',
                                     description='This program produces an action plan for an environmental environmental  agency interested in monitoring the actions of mine owners')
    parser.add_argument("--md", help="Reload the mine data from wikipedia", action="store_true")
    parser.add_argument("--ms", help="Reload the mine sizes from earth engine", action="store_true")
    parser.add_argument("--rs", help="Recalculate all R-Squared values", action="store_true")
    parser.add_argument("--verbose", "-v", help="Verbose output", action="count")
    parser.add_argument("--cache", help="Use the specified cache file", type=str, action="store", default='mineData.dump')
    parser.add_argument("--start-date", help="The earliest date to use in constructing the model",
                        type=str, action="store", default='2008-01-01')
    parser.add_argument("--end-date", help="The earliest date to use in constructing the model",
                        type=str, action="store", default='2015-05-01')

    parser.add_argument("--startdate", help="The earliest date to use in constructing the model",
                        type=str, action="store", default='2008-01-01')
    parser.add_argument("--enddate", help="The earliest date to use in constructing the model",
                        type=str, action="store", default='2015-05-01')

    parser.add_argument("--minesize", help="The size of the region to look for the mine in (degrees)",
                        type=float, action="store", default='0.025')
    parser.add_argument("--controlsize", help="The size of the control region around the mine to factor out seasonal variation (degrees)",
                        type=float, action="store", default='0.05')

    #parser.add_argument("--nocache", help="Do not use a cache file (Not recommended)", type=str, action="store_const", const=None)

    parser.add_argument("gold", help="The current price of gold ($/oz)", type=float, action="store")
    parser.add_argument("silver", help="The current price of silver ($/oz)", type=float, action="store")
    parser.add_argument("uranium", help="The current price of uranium ($/lb)", type=float, action="store")
    parser.add_argument("coal", help="The current price of coal ($/tonne)", type=float, action="store")
    parser.add_argument("copper", help="The current price of copper ($/tonne)", type=float, action="store")
    parser.add_argument("lead", help="The current price of lead ($/tonne)", type=float, action="store")

    parser.add_argument("days", help="The number of days in the future to produce an action plan for",
                        type=int, action="store")

    args = parser.parse_args()

    global startDate
    startDate = args.startdate
    global endDate
    endDate = args.enddate

    currentCommodityPrices = { 'Gold' : args.gold, 'Silver' : args.silver, 'Coal' : args.coal,
                               'Uranium' : args.uranium, 'Lead' : args.lead, 'Copper' : args.copper}

    #Attempt to load data from the cache
    mineList = None
    if os.path.isfile(args.cache) and not (args.md and args.ms and args.rs):
        #Load the saved data
        fp = open(args.cache, 'r')
        mineList = json.load(fp, cls=PandasDateDecoder)
        fp.close()

    #Get Mine location Data
    if args.md or mineList is None:
        mineNames = fetchMineNames()
        mineList = []
        for mineName in mineNames:
            mineLocation, mineProducts = fetchMineData(mineName)
            if(mineLocation is not None and mineProducts):
                mineList.append({ 'location' : mineLocation, 'products' : mineProducts })
                if args.verbose >= 1:
                    print(mineName + " (lat: " + str(mineLocation[0]) + ", lon: " + str(mineLocation[1]) + "), which produces: " + ", ".join(mineProducts) + ".")
        #Save the data so it doesn't need loading next time
        fp = open(args.cache, 'w')
        json.dump(mineList, fp, cls=PandasDateEncoder)
        fp.close()

    if mineList is None :
        raise ValueError("Could not load the list of mines from wikipedia or local storage")

    #Get Mine Size Data from Earth Engine
    ee.Initialize()
    for mineData in mineList:
        if args.ms or 'growth' not in mineData:
            eviCollection = ee.ImageCollection("MODIS/MCD43A4_EVI")
            mineVegetation = mineChangeTime(eviCollection,
                                            mineData['location'][1], mineData['location'][0], args.minesize)
            controlVegetation = mineChangeTime(eviCollection,
                                               mineData['location'][1], mineData['location'][0], args.controlsize)
            mineData['growth'] = []
            lastSSS = None
            for mineSize, controlSize in zip(mineVegetation, controlVegetation):
                if(mineSize[1] is not None and controlSize[1] is not None):
                    seasonScaledSize = 1 - (mineSize[1] / (controlSize[1] - mineSize[1] + 0.1))
                    curDate = pandas.to_datetime(mineSize[0], unit='ms')
                    try:
                        if args.verbose >= 1:
                            #Verbose output in csv format of mine size and prices at each timestep
                            printBase = str(mineSize[0]) + ", " + str(seasonScaledSize)
                            for commodity in mineData['products']:
                                cPrice = getCommodityPrice(commodity, curDate)
                                printBase = printBase + ", "+ str(cPrice)
                            print printBase
                        if lastSSS is not None:
                            #This check exists to make sure that commodity date is available for this data point
                            getCommodityPrice(commodity, curDate)
                            mineData['growth'].append(((seasonScaledSize - lastSSS) / (curDate - lastDate).days, curDate))
                        lastDate = curDate
                        lastSSS = seasonScaledSize
                    except:
                        if args.verbose >= 2:
                            print "Lost the datapoint at " + str(curDate) + " due to a lack of commodity data"
            #Scale the
            minPreScale = np.percentile([sss for sss, _ in mineData['growth']], 5)
            maxPreScale = np.percentile([sss for sss, _ in mineData['growth']], 95)
            mineData['growth'] = [(min(1, max(0, (sssDiff-minPreScale) / (maxPreScale - minPreScale))), date)
                                for sssDiff, date in mineData['growth']]
        if 'growth' not in mineData or len(mineData['growth']) == 0:
            raise ValueError("Could not load the size of a mine from earth engine or local storage")
        #save the mine data from earth engine for later
        fp = open(args.cache, 'w')
        json.dump(mineList, fp, cls=PandasDateEncoder)
        fp.close()

    #Calculate r^2 values for prices vs growth for each mine
    for mineData in mineList:
        if args.rs or 'RSquared' not in mineData:
            mineData['RSquared'] = {}
            for commodity in mineData['products']:
                mineData['RSquared'][commodity] = {}
                sizes =[]
                prices =[]
                for mineGrowth, date in mineData['growth']:
                    if pandas.Timestamp(date) < pandas.Timestamp(endDate):
                        sizes.append(mineGrowth)
                        prices.append(getCommodityPrice(commodity, date))

                if args.verbose >= 2:
                    fig, ax = plt.subplots()
                    line, = ax.plot(prices, sizes, linestyle = 'None', marker='o')
                    ax.set_xlim([min(prices),max(prices)])
                    ax.set_ylim([min(sizes), max(sizes)])
                    fig.suptitle(commodity)
                    plt.show()

                mineData['RSquared'][commodity][0] = sp.stats.linregress(prices, sizes)[2]**2

                for offset in np.arange(1, args.days + 1):
                        sizes =[]
                        prices =[]
                        for mineGrowth, date in mineData['growth']:
                            if pandas.Timestamp(date) <  pandas.Timestamp(endDate):
                                sizes.append(mineGrowth)
                                prices.append(getCommodityPrice(commodity, pandas.Timestamp(date) - pandas.to_timedelta( offset, unit='d')))
                        mineData['RSquared'][commodity][offset] = sp.stats.linregress(prices, sizes)[2]**2

    #Print r^2 data so it can be visualised
    if args.verbose >= 1:
        print "Day offset," + ','.join(commodityTypes)
        for offset in np.arange(args.days +1):
            rs = [str(offset)]
            for commodity in commodityTypes:
                cModel = bestPrediction(mineList, commodity, offset)
                if cModel:
                    rs.append(str(cModel[2]**2))
                else:
                    rs.append(str(0))
            print ','.join(rs)

    #Generate the best models for each commodity, and use it to estimate growth
    commodityGrowth = {}
    for commodity in commodityTypes:
        cModel = bestPrediction(mineList, commodity, args.days)
        if cModel is not None:
            commodityGrowth[commodity] = cModel[1] + cModel[0] * currentCommodityPrices[commodity]
            if args.verbose >= 1:
                print commodity + ", " + str(commodityGrowth[commodity])

    #print out the action plan
    print "The highest risk types of mine are, from highest to lowest risk: "
    for commodity, risk in sorted(commodityGrowth.items(), key=lambda x: x[1], reverse = True):
        print commodity + ", (estimated growth rate: " + str(commodityGrowth[commodity]) + ", in arbitrary units)"

#Allows pandas dates to be stored in the JSON
class PandasDateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pandas.Timestamp):
            return obj.strftime("%Y-%m-%d")

        return json.JSONEncoder.default(self, obj)

#Allows pandas dates to be loaded from the JSON
class PandasDateDecoder(json.JSONDecoder):
    def __init__(self, *args, **kargs):
        json.JSONDecoder.__init__(self, object_hook=self.dict_to_object, *args, **kargs)
    def dict_to_object(self, d):
        if '__type__' not in d:
            return d

commodityPrices = {}
#Gets the price of a commodity on the given date
def getCommodityPrice(type, date):
    global commodityTypes
    global startDate
    global endDate
    global commodityPrices

    if type not in commodityTypes :
        raise NameError(str(type) + ' is not a valid commodity.')

    #Check if this commodity has been loaded and interpolated yet, lazy initialization
    if(type not in commodityPrices):
        #Find bufferedInclusive ranges to make sure theres enough data for interpolation, and
        #buffered ranges to make sure theres enough data for modis values outside of the given range
        startDateBuffered = str(pandas.Timestamp(startDate) + pandas.Timedelta(days=-93))
        startDateBufferedInclusive = str(pandas.Timestamp(startDate) + pandas.Timedelta(days=-124))
        endDateBuffered = str(pandas.Timestamp(endDate) + pandas.Timedelta(days=124))
        endDateBufferInclusive = str(pandas.Timestamp(endDate) + pandas.Timedelta(days=155))

        #Get the data!
        data = Quandl.get(commodityTypes[type], returns='numpy', authtoken= quandlKey, trim_start=startDateBufferedInclusive,
                          trim_end=endDateBufferInclusive, exclude_column_names = True)

        #Interpolate data to get daily values
        lastIndex = 0
        curIndex = 0
        commodityPrices[type] = {}
        for curDate in pandas.date_range(startDateBuffered, endDateBuffered):
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

    #Convert date to a pandas timestamp if possible
    if isinstance(date, str)or isinstance(date, unicode):
        date = pandas.Timestamp(date)
    elif not isinstance(date, pandas.Timestamp):
        raise TypeError('Unknown date type')

    if date not in commodityPrices[type]:
        raise ValueError('Given date not within the valid range')

    return commodityPrices[type][date]

#Create a predictor based for the desired commodity and time offset, using the best combination of data
def bestPrediction(mineList, commodity, days):
    bestModelSizes = []
    bestModelPrices = []
    bestModelRSquared = -1
    producingMines = [x for x in mineList if commodity in x['RSquared'] and days in x['RSquared'][commodity] ]
    if len(producingMines) == 0:
        return None
    for mineData in sorted(producingMines, key=lambda x: x['RSquared'][commodity][days]):
        rollbackSizes = bestModelSizes
        rollbackPrices = bestModelPrices
        rollbackRSquared = bestModelRSquared
        for mineGrowth, date in mineData['growth']:
            bestModelSizes.append(mineGrowth)
            bestModelPrices.append(getCommodityPrice(commodity, date))
        if days == 0:
            bestModelRSquared = sp.stats.linregress(bestModelPrices, bestModelSizes)[2]**2
        else:
            bestModelRSquared = sp.stats.linregress(bestModelPrices[:-days], bestModelSizes[days:])[2]**2
        if rollbackRSquared > bestModelRSquared:
            bestModelSizes = rollbackSizes
            bestModelPrices = rollbackPrices
    if days == 0:
            return sp.stats.linregress(bestModelPrices, bestModelSizes)
    else:
        return sp.stats.linregress(bestModelPrices[:-days], bestModelSizes[days:])

#Get a list of mine names from wikipedia
def fetchMineNames():
    page, _ = wikipediaRequest("List_of_open-pit_mines")
    r = re.compile("^\*[^\[\]]*?\[\[(?P<mineName>[^\[\]\|]+)", re.MULTILINE)
    return [m.groupdict()["mineName"] for m in r.finditer(page)]

#Make a request for a specific webpage from wikipedia
def wikipediaRequest(article):
    #Convert unicode to ASCII, make the request
    requestUrl = wikipediaBase + urllib2.quote(article.encode('utf8'), '-')
    req = urllib2.Request(requestUrl, headers = wikiUAHeader)
    url = urllib2.urlopen(req)

    #Convert ascii response to unicode and get the data
    queryResponse = json.loads(url.read().decode('utf-8'))['query']
    pageData = list(queryResponse['pages'].values())[0]

    #Deal with redirects and moved pages if neccesary
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

#Get the location and mined minerals for the mine with the given name, from wikipedia
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

#Contact earth engine and get the amount of vegetation over a region, in a time series
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
