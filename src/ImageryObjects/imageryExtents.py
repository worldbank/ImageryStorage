###############################################################################
# Raster Folder Extents
# Benjamin P. Stewart, March 2017
# Purpose: extract a zipfile of delivered imagery and extract imagery extents
###############################################################################

import os, sys, zipfile, json, shutil, logging
import rasterio, pyproj, geohash

import xml.etree.ElementTree as ET 
import geopandas as gpd
import pandas as pd
import numpy as np

from datetime import datetime
from functools import partial
from shapely.geometry import Polygon, LineString, Point
from PIL import Image
from shapely.ops import transform
from zipfile import ZipFile


Image.MAX_IMAGE_PIXELS = 89478485000000
vendorInformation = {
    "Maxar":{'browseTag':'BROWSE.JPG'},
    'SPOT':{'browseTag':'PREVIEW'},
    'GOST':{'browseTag':''},
    'gbdx_clip':{'browseTag:'}
}
allFileTypes = ["TIF", "tif"]

class deliveredImageryFolder(object):

    def __init__(self, inputFolder, outputFolder, adminBoundaries, vendor = ''):
        ''' Generate metadata for input images in folder
        
        Parameters:
            inputFolder (string): folder to process
            sensor (string): sensor to process - used to define where to find important metadata        
            adminBoundaries (geopandas geodataframe): file path to shapefile of global bouncaries for finding country of interest
        '''
        self.inputFolder = inputFolder
        self.outputFolder = outputFolder
        if vendor == '':
            self.vendor = self.determineVendor()
        else:
            self.vendor = vendor        
        if self.vendor == "gbdx_clip":
            self.process_gbdx_xml()        
        self.adminBoundaries = adminBoundaries
        self.findImages()
        self.zipFile = os.path.join(outputFolder, "%s.zip" % self.generateFilename()) 
        self.jsonFile = os.path.join(outputFolder, "%s.json" % self.generateFilename())
        self.thumbnail = os.path.join(outputFolder, "%s.jpg" % self.generateFilename())
    
    def process_gbdx_xml(self):
        ''' Images downloaded through gbdx task clipping come with an XML that should have the information we need
        
        RETURNS
        NULL - sets a number of object variables, such as date
        '''
        inFiles = os.listdir(self.inputFolder)
        if "gbdx_clip_log.txt" in inFiles:
            for f in inFiles:
                if f[-4:] == ".XML":
                    xml = ET.parse(os.path.join(self.inputFolder, f))
                    root_element = xml.getroot()
                    self.official_id = root_element.findall('IMD')[0].findall('IMAGE')[0].findall('CATID')[0].text
                    date = root_element.findall('IMD')[0].findall('IMAGE')[0].findall('TLCTIME')[0].text
                    dTime = datetime.strptime(date[:10], "%Y-%m-%d")
                    cDate = dTime.strftime("%Y%m%d")
                    self.Date = cDate
    
    def determineVendor(self):
        ''' Based on folder and file structure, determine the vendor for other calculations
        
        RETURNS
        [string] - vendor name matching imageryExtents.vendorInformation
        '''
        inFiles = os.listdir(self.inputFolder)
        if "gbdx_clip_log.txt" in inFiles:
            return("gbdx_clip")
        for f in inFiles:
            if f[-10:] == "BROWSE.JPG":
                return("MAXAR")
    
    def createJSON(self, pNumber, securityClassification="Official Use Only"):
        if not os.path.exists(self.jsonFile):
            m = self.allMetadata
            try:
                zippedSize = os.path.getsize(self.zipFile)
            except:
                zippedSize = 0
            self.metadataDictionary = {
                "title":"Satellite imagery for %s" % self.countryISO3,
                "iso3":self.countryISO3,
                "location":self.zipFile,
                "zippedSize":zippedSize,
                "resolution":",".join([str(x) for x in np.unique([m['Res'].min(), m['Res'].max()])]),
                "nBands":",".join([str(x) for x in np.unique([m['Bands'].min(), m['Bands'].max()])]),
                "vendor":self.vendor,
                "capture_date":",".join([str(x) for x in np.unique([m['Date'].min(), m['Date'].max()])]),
                "pNumber":pNumber,
                "securityClassification":securityClassification,
                "ImageExtent":self.allMetadata.unary_union.wkt,
                "originalLocation":self.inputFolder
                #"imageTileDetails":self.allMetadata
            }
            with open(self.jsonFile, 'w') as j:
                json.dump(self.metadataDictionary, j)
    
    def zipData(self):    
        #Get a list of all files
        if not os.path.exists(self.zipFile):
            allFiles = []            
            for root, dirs, files in os.walk(self.inputFolder):
                for f in files:
                    allFiles.append(os.path.join(root, f))
            
            with ZipFile(self.zipFile, 'w') as zip:
                for f in allFiles:
                    zip.write(f)
        return(self.zipFile)
    
    def getDate(self, file):
        ''' Get date of image, depending on Vendor
        '''
        try:
            return(self.Date)
        except:
            cDate = "YYYYMMDD"
            if self.vendor == "MAXAR":
                try:
                    date = "20%s" % os.path.basename(file)[:7]
                    dTime = datetime.strptime(date, "%Y%b%d")
                    cDate = dTime.strftime("%Y%m%d")                    
                except:
                    logging.warning("Could not determine date for %s" % file)                    
            self.Date = cDate
            return(self.Date)
    
    def identifyCountries(self):
        try:
            allM = self.allMetadata
        except:
            allM = self.getMetadata()
        inputExtent = allM.unary_union
        
        country = self.adminBoundaries[self.adminBoundaries.intersects(inputExtent)]
        self.countryName = ";".join(country['WB_ADM0_NA'])
        self.countryISO3 = ";".join(country['ISO3'])    
        
    def getMetadata(self):
        ''' Generate metadata dataframe for every image
        '''
        try:
            return(self.allMetadata)
        except:        
            allRes = []
            for x in self.allImages:            
                curRaster = rasterio.open(x)          
                #Extract imagery extent
                if curRaster.crs:
                    crs = curRaster.crs
                    b = curRaster.bounds
                    bbox = [[b.left, b.bottom],
                            [b.left, b.top],
                            [b.right, b.top],
                            [b.right, b.bottom],
                            [b.left, b.bottom]]
                    bbox = Polygon(bbox)
                    project = partial(
                        pyproj.transform,
                        pyproj.Proj(init=str(crs)), # source coordinate system
                        pyproj.Proj(init='epsg:4326')) # destination coordinate system
                    res = curRaster.res[0]
                    bbox2 = transform(project, bbox)               
                    #Need to transform resolution to a meters measure
                    if curRaster.crs.to_epsg() == 4326:                        
                        project = partial(
                            pyproj.transform,
                            pyproj.Proj(init='epsg:4326'), # source coordinate system
                            pyproj.Proj(init='epsg:3857')) # destination coordinate system
                        l = LineString([Point(0,0), Point(0,res)])
                        l = transform(project, l)
                        res = l.length
                        
                    allRes.append([curRaster.count, round(res, 3), 
                                    bbox2, geohash.encode(bbox2.centroid.y, bbox2.centroid.x), 
                                    curRaster.shape[0], curRaster.shape[1], self.getDate(x), x])
            
            #create output metadata dataFrame
            if len(allRes) > 0:
                allMetadata = pd.DataFrame(allRes, columns=["Bands", "Res", "geometry", "geohash", "columns", "rows", "Date","file"])
                allMetadata = gpd.GeoDataFrame(allMetadata, geometry="geometry", crs=pyproj.Proj(init='epsg:4326'))
                if allMetadata.iloc[0]['Date'] == "YYYYMMDD":
                    try:
                        allMetadata['Date'] = self.Date
                    except:
                        pass
                self.allMetadata = allMetadata
                return(self.allMetadata)
            else:
                raise(ValueError("Folder does not have any valid raster datasets"))
        
    def generateFilename(self):
        ''' Generate a single filename based on input data       
        '''
        try:
            return(self.fileName)
        except:
            try:
                allM = self.allMetadata
            except:
                allM = self.getMetadata()
            self.identifyCountries()
            u = allM.unary_union  
            try:            
                gHash = geohash.encode(u.centroid.y,u.centroid.x)
            except:
                gHash = "XXXXXXXXX"
            # Generate a filename 
            outString = "{ISO3}_{gHash}_{bands}_{res}_{curDate}".format(
                ISO3 = self.countryISO3,
                gHash = gHash,
                res = ",".join([str(x) for x in np.unique([allM['Res'].min(), allM['Res'].max()])]),
                bands = ",".join([str(x) for x in np.unique([allM['Bands'].min(), allM['Bands'].max()])]),
                curDate = ",".join([str(x) for x in np.unique([allM['Date'].min(), allM['Date'].max()])])
            )
            self.fileName = outString
        return(self.fileName)
            
    
    def findImages(self, filetype="TIF"):
        ''' Get list of imagery files in the input folder
        
        Parameters:
            filetype (string) (options): filetype to look for
        
        Returns:
            list of images
        '''
        self.allImages = []
        for root, dirs, files in os.walk(self.inputFolder):
            for f in files:                
                for filetype in allFileTypes:
                    if f[-len(filetype):] == filetype:
                        # BEN TODO: Should I check if this is an actual raster?
                        self.allImages.append(os.path.join(root, f))
                
    def generateThumbnails(self, filetype="TIF"):
        ''' Generate thumbnails for all images in folder
        
        #TODO - needs more testing
        for x in self.allImages:            
            outFile = os.path.join(self.thumbnailFolder, os.path.basename(x).replace(filetype, "jpg"))
            if not os.path.exists(outFile):
                try:
                    im = Image.open(x)
                    im.thumbnail((round(im.size[0]/10), round(im.size[1]/10)), Image.ANTIALIAS)
                    im.save(outFile, "JPEG")
                except:
                    logging.warning("Problem generating thumbnail for %s" % os.path.basename(x))
        '''      
        try:
            if not os.path.exists(self.thumbnail):
                for root, dirs, files in os.walk(self.inputFolder):
                    for f in files:
                        if self.vendor == "Maxar":
                            #Look for a BROWSE.jpg
                            if vendorInformation[self.vendor]['browseTag'] in f:
                                shutil.copyfile(os.path.join(root, f), self.thumbnail)
                            else:
                                logging.info("could not find thumbnail for %s" % self.inputFolder)
        except:
            logging.warning("Could not find thumbnail for %s" % self.inputFolder)
    
    def getImageryExtents(self, inFolder):
        '''Get a list of input imagery tiles and generate extent dataframes
        RETURNS [geopandas dataframe] - Each row contains file path and shape extent for every imagery tile
        '''
        try:
            return(self.imageryExtents)
        except:
            allExtents = []
            crs = ''
            for curF in self.allImages:
                try:
                    #Try to open the file as a rasterio object
                    curRaster = rasterio.open(curF)
                    if curRaster.crs:
                        crs = curRaster.crs
                        b = curRaster.bounds
                        bbox = [[b.left, b.bottom],
                                [b.left, b.top],
                                [b.right, b.top],
                                [b.right, b.bottom],
                                [b.left, b.bottom]]
                        allExtents.append([f, Polygon(bbox), curRaster.res[0]])                    
                except Exception as e:
                    logging.warning("Problem generating extent for %s" % os.path.basename(x))
                
            if crs != '':
                curDf = gpd.GeoDataFrame(allExtents, crs=crs, columns=["fileName","geometry",'Resolution'])
                if curDf.crs != {'init': 'epsg:4326'}:
                    curDf = curDf.to_crs({'init': 'epsg:4326'})        
                curDf['zipFile'] = self.zipFile
                curDf = curDf.set_geometry("geometry")
                self.imageryExtents = curDf
                return curDf
            else:
                return -1           
    
    def valid_metadata(self, metadata):
        ''' Determine if the metadata is sufficient for cataloging
        
        RETURNS
        [boolean] - if TRUE, metadata should be created
        '''
        goodData = True
        #Check to see if the date field is populated
        for idx, row in metadata.iterrows():
            if row['Date'] == "YYYYMMDD":
                goodData = False
        return(goodData)    
        
        
class zipFileExtents(object):
    def __init__(self, inputZip, sensor, sourceFolder):
        ''' Generate object for extracting imagery metadata
        INPUT
        inputZip [string] - path to imagery zipFile
        sensor [string] - sensor name
        '''
        self.zipFile = inputZip        
        self.sensor = sensor
        self.sourceFolder = sourceFolder

        
        ###TODO - should this check been done here???
        ### if not os.path.exists(xx.extentsFile):
        #Check if data has been extracted
        if self.checkForSource():
            #Get imagery extents
            xy = self.fileNames
        else:
            IShould = "Raise an error here"

    def unzip(self):
        if not self.checkForSource():
            zFile = zipfile.ZipFile(self.zipFile)
            zFile.extractall(self.sourceFolder)
            return(2)
        else:
            return(1)
    
    def checkForSource(self):  
        ''' Determine that the VENDOR_DISTRIBUTION zipfiles have been properly extracted and processed to SOURCE
                TODO: Integrate the use of an actual log as an input
                    Begin by reading in the catalog, then reading in the ...
        '''
        self.sourceExists = False
        self.getSourceNames()
        
        #Search the input sourceFolder for the files
        for roots, dirs, files in os.walk(self.sourceFolder):
            for name in files:
                if name in self.fileNames.keys():
                    self.fileNames[name] = True    
        if len(self.fileNames.values()) > 0:
            self.sourceExists = (float(sum(self.fileNames.values())) / float(len(self.filePaths))) > 0.50
        else:
            self.sourceExists = False
        return self.sourceExists
    
    def getSourceNames(self):       
        '''Get the filenames from the input zipFile'''
        try:
            return(self.fileNames)
        except:
            curZip = zipfile.ZipFile(self.zipFile)        
            self.filePaths = curZip.namelist()        
            self.fileNames = {}
            for f in self.filePaths:
                self.fileNames[os.path.basename(f)] = False
            return(self.fileNames)
    
    def getImageryExtents(self, inFolder):
        '''Get a list of input imagery tiles and generate extent dataframes
        RETURNS [geopandas dataframe] - Each row contains file path and shape extent for every imagery tile
        '''
        try:
            return(self.imageryExtents)
        except:
            if self.checkForSource():
                allExtents = []
                crs = ''
                for dir, subDirList, fileList in os.walk(inFolder):   
                    for f in fileList:
                        curF = os.path.join(dir, f)
                        try:
                            #Try to open the file as a rasterio object
                            curRaster = rasterio.open(curF)
                            if curRaster.crs:
                                crs = curRaster.crs
                                b = curRaster.bounds
                                bbox = [[b.left, b.bottom],
                                        [b.left, b.top],
                                        [b.right, b.top],
                                        [b.right, b.bottom],
                                        [b.left, b.bottom]]
                                allExtents.append([f, Polygon(bbox), curRaster.res[0]])
                            
                        except Exception as e:
                            FUBAR = "DO Nothing"
                    
                if crs != '':
                    curDf = gpd.GeoDataFrame(allExtents, crs=crs, columns=["fileName","geometry",'Resolution'])
                    if curDf.crs != {'init': 'epsg:4326'}:
                        curDf = curDf.to_crs({'init': 'epsg:4326'})        
                    curDf['zipFile'] = self.zipFile
                    curDf = curDf.set_geometry("geometry")
                    self.imageryExtents = curDf
                    return curDf
                else:
                    return -1           
            else:
                raise ValueError("Input imagery has not been unzipped")
        