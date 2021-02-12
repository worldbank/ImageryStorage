import logging, sys, os, inspect, glob, datetime, time, shutil
import timeit
import shapely.wkt

import pandas as pd
import geopandas as gpd

start = timeit.default_timer()
cmd_folder = os.path.dirname(os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

import imageryExtents

logging.basicConfig(filename="C:/Work/errors.log")

inputFolder = r"\\ddhprdcifs\DDH-PRD\ddhfiles\internal\imagerysource\Vendor_Distributions"

allFiles = []
for dirName, subdirList, fileList in os.walk(inputFolder):
    for f in fileList:        
        if f[-4:] == ".zip":        
            allFiles.append(os.path.join(dirName, f))

#List all existing extents files
extentsFiles = glob.glob(r"\\ddhprdcifs\DDH-PRD\ddhfiles\internal\imagerysource\Source\*\*\imageryExtents.csv")
for f in extentsFiles:
    curPD = pd.read_csv(f)
    if f == extentsFiles[0]:
        final = curPD
    else:
        final = final.append(curPD)

#final = final.reset_index()
final['newIdx'] = range(0, final.shape[0])
final = final.set_index('newIdx')

inVal = final[final.fileName == 'Gonaives.tif']
def processGeo(inVal):
    try:
        #Re-project the data if it is not WGS84
        curGeom = shapely.wkt.loads(str(inVal.geometry))
        inVal = inVal.drop("geometry")
        curCrs = inVal.CRS.replace("CRS({'init': u'", "").replace("'})","")
        curG = gpd.GeoDataFrame([inVal], crs={'init': '%s' % curCrs}, geometry=[curGeom])
        curG = curG.to_crs({'init': u'epsg:4326'})  
        inVal.CRS = "CRS({'init': u'epsg:4326'})"
        inVal['geo_wgs84'] = curG['geometry'].iloc[0]
        b = curG.bounds
        inVal['xmin'] = b.minx.iloc[0]
        inVal['ymin'] = b.miny.iloc[0]
        inVal['xmax'] = b.maxx.iloc[0]
        inVal['ymax'] = b.maxy.iloc[0]
        return inVal
    except:
        return inVal

finalGeo = final.apply(processGeo, axis=1) 
finalGeo = finalGeo[(finalGeo.xmin > -180) & (finalGeo.xmax < 180) & (finalGeo.ymin > -90) & (finalGeo.ymax < 90)] 

#Groupby the zipFiles and merge 
finalZ = None
for x in finalGeo.groupby(["zipFile", "CRS"]):
    try:
        #Create a geodataFrame from the grouped data
        curS = gpd.GeoDataFrame(x[1], crs={'init':'epsg:4326'}, geometry='geo_wgs84')
        curS_dissolve = curS.dissolve(by="zipFile")
        try:
            finalZ = finalZ.append(curS_dissolve)
        except:
            finalZ = curS_dissolve
    except:
        print("Error processing %s" % x[1].zipFile.iloc[0])

finalZ.to_csv("C:/Temp/imageryExtents.csv")
   
for f in allFiles:
    #Create the zipFileExtents object
    xx = imageryExtents.zipFileExtents(f)
    if not os.path.exists(xx.extentsFile):
        currentExtent = xx.getImageryExtents()
        try:
            currentExtent.to_csv(xx.extentsFile)
            print "processed %s" % f
        except:
            print "%s cannot be analyzed" % f              
    else:
        print "%s already processed" % xx.extentsFile
        currentExtent = pd.read_csv(xx.extentsFile)        
    #Combine the imagery extents
    if f == allFiles[0]:
        final = currentExtent
    else:     
        try:
            final = final.append(currentExtent)
        except:
            logging.info("%s was not processed" % f)
            
