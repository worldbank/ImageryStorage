###############################################################################
# Imagery Upload Object
# Benjamin P. Stewart, August 2017
# Purpose: Evaluate the status of each of the Imagery ZipFiles on our imagery service
#TODO:  Separate out the cataloguing into its own function
#           Create a catalog for each service, then tabulate into a single file
#       Integrate the use of an actual log as an input
#           Begin by reading in the catalog, then reading in the 
###############################################################################

import os, sys, csv, inspect, datetime, shutil, subprocess, time, glob
#import arcpy
import zipfile, tarfile

'''prints the time along with the message'''
def tPrint(s):
    print"%s\t%s" % (time.strftime("%H:%M:%S"), s)

class imageryZip(object):  
    def getSourceNames(self):       
        if not self.sourceExists:
            #Check if the source files have been extracted and processed
            curZip = zipfile.ZipFile(self.zipFile)
            self.filePaths = curZip.namelist()
        else:
            self.filePaths = glob.glob("%s/*.*" % self.sourceFolder)
        self.fileNames = {}
        for f in self.filePaths:
            self.fileNames[os.path.basename(f)] = False
            
    def checkForSource(self):  
        ''' Determine that the VENDOR_DISTRIBUTION zipfiles have been properly extracted and processed to SOURCE
                TODO: Integrate the use of an actual log as an input
                    Begin by reading in the catalog, then reading in the 
        '''
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
                    
    def checkForService(self):
        ''' Check if the source has been properly uploaded to the imagery catalogue
        
        '''
        self.serviceOID = ''
        self.sourceService = ''
        self.rgbOID = ''
        self.rgbServiceExists = False
        if arcpy.Exists(self.gdbPath):
            result = arcpy.GetCount_management(self.gdbPath)
            result = int(result.getOutput(0))
            if result > 0:
                #Export the catalog for the current sensor
                if not os.path.isfile(self.rasterCatalog): 
                    arcpy.ExportRasterCatalogPaths_management(self.gdbPath, "ALL", self.rasterCatalog)
                #Export the catalog for the D_RGB service as well
                if not os.path.isfile(self.rgbCatalog):   
                    arcpy.ExportMosaicDatasetPaths_management(self.rgbPath, self.rgbCatalog, export_mode="ALL", types_of_paths="RASTER;ITEM_CACHE")                                                                                  
                    #arcpy.ExportMosaicDatasetPaths_management(in_mosaic_dataset="//ddhprdcifs/DDH-PRD/ddhfiles/internal/imagerysource/FGDB/ImageryServices.gdb/S_RGB", out_table="//ddhprdcifs/DDH-PRD/ddhfiles/internal/imagerysource/Logs/CatalogInventory/2017_09_25_RGB_catalog.dbf", where_clause="", export_mode="ALL", types_of_paths="RASTER;ITEM_CACHE")
                self.serviceFiles = {}
                #for catPath in [self.rasterCatalog, self.rgbCatalog]:
                with arcpy.da.SearchCursor(self.rasterCatalog, ["SourceOID","Path"]) as searchCur:
                    for row in searchCur:
                        self.serviceFiles[os.path.basename(row[1])] = [row[0], "S_%s" % self.sensor]
                with arcpy.da.SearchCursor(self.rgbCatalog, ["SourceOID","Path"]) as searchCur:
                    for row in searchCur:
                        self.serviceFiles[os.path.basename(row[1])] = [row[0], "S_RGB"]

                #Check if the service has been created
                self.serviceExists = False
                if not hasattr(self, 'fileNames'):
                    self.checkForSource()
                for fileName, fileDetails in self.serviceFiles.iteritems():
                    if fileName in self.fileNames:
                        self.serviceOID = fileDetails[0]
                        self.sourceService = fileDetails[1]
                        self.serviceExists = True
                        self.sourceExists = True    #There are odd situations where the source is not marked as true
            else:
                self.serviceExists = False
        else:
            self.serviceExists = False
            
        #Identify the path in the D_RGB service as well
        if arcpy.Exists(self.drgbPath) and self.serviceExists:
            if not os.path.isfile(self.drgbRasterCatalog):   
                arcpy.ExportRasterCatalogPaths_management(self.drgbPath, "ALL", self.drgbRasterCatalog)
                rgbFiles = self.serviceFiles
                with arcpy.da.SearchCursor(self.drgbRasterCatalog, ["SourceOID","Path"]) as searchCur:
                    for row in searchCur:
                        rgbFiles[os.path.basename(row[1])] = [row[0], "D_RGB"]

                #Check if the service has been created
                self.serviceExists = False
                for fileName, fileDetails in rgbFiles.iteritems():
                    if fileName in self.fileNames:
                        self.rgbOID = fileDetails[0]
                        self.rgbServiceExists = True
                        
                
            
    def extractandTile(self):
        f = self.zipFile
        ingestFolder = self.ingestFolder
        sourceFolder = self.sourceFolder
        if f[-3:] == "zip":        
            #Extract the zipfile
            curZip = zipfile.ZipFile(f, 'r')
            curZip.extractall(ingestFolder)
            curZip.close()            
        if f[-2:] == "gz":
            #Extract the zipfile
            curTar = tarfile.open(f, 'r:gz')
            curTar.extractall(ingestFolder)
            curTar.close()
            
        for dirName, subdirList, fileList in os.walk(ingestFolder):
            #Run through and copy all the files in the fileList
            for f in fileList:
                #If the file is xml, kml, or gml, copy it to the output folder
                outputFolder = dirName.replace("Ingest", "Source")
                if not os.path.exists(outputFolder):
                    os.makedirs(outputFolder)
                if f[-4:].lower() in [".tif"]:                        
                    outputRasterFile = self.tileAndOverview(dirName, f, self.gdalRoot)                                
                else:
                    shutil.copyfile(os.path.join(dirName, f), os.path.join(outputFolder, f))                            
        shutil.rmtree(ingestFolder)

    def tileAndOverview(self, inPath, inFile, gdalPath):
        outPath = inPath.replace("Ingest", "Source")
        rasterFile = os.path.join(inPath, inFile)
        outRasterFile = os.path.join(outPath, inFile)
        
        #create output folder
        if not os.path.isdir(outPath):
            os.makedirs(outPath)

        #run gdal_translate to tile image properly
        if not os.path.exists(outRasterFile):
            processCall = '%s -of Gtiff -co "TILED=YES" -co "BLOCKXSIZE=256" -co "BLOCKYSIZE=256" "%s" "%s"'  % (os.path.join(gdalPath, "gdal_translate.exe"), rasterFile, outRasterFile)
            subprocess.call(processCall)

            #Calculate Overviews (Pyramids) on data in source folder
            overviewCall = '"%s" -r average --config INTERLEAVE_OVERVIEW PIXEL --config PHOTOMETRIC_OVERVIEW YCBCR "%s" 2 4 8 16 32 64 128 256' \
                % (os.path.join(gdalPath, "gdaladdo.exe"), outRasterFile)    
            subprocess.call(overviewCall)
            
        return outRasterFile
    
    def uploadSourceData(self, uMS="false", uPS="false", uPan="false"):
        '''
        http://www.arcgis.com/home/item.html?id=b7c019a561fc427a8ebf9d463df41637
        '''
                
        #Import the toolboxes, if they aren't in the list of imported toolboxes
        if not "HighResolution" in arcpy.ListToolboxes():
            arcpy.ImportToolbox(self.arcToolbox_HighRes)
        if not "Preprocess" in arcpy.ListToolboxes():
            arcpy.ImportToolbox(self.arcToolbox_orthos)
        inRaster = self.sourceFolder
        sensorName = self.sensor
        outputWorkspace = self.gdbPath
        zipLocation = self.zipFile        
        if sensorName == "SPOT5": 
            '''Just for SPOT5 data, the delivered zipfile isn't the exact folder that needs to be uploaded. 
            Instead, we need a reference to the single folder inside of that'''
            inRaster = "%s/%s" % (inRaster, os.listdir(inRaster)[0])               
        try:
            if sensorName == "PLEIADES":
                arcpy.Pleiades(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_PLEIADES", input_path=inRaster, DOBO="false", dem_path="", MS=uMS, PS=uPS, Pan=uPan)
                self.serviceExists = True
                return(1)
            if sensorName == "GEOEYE":
                arcpy.Geoeye(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_GEOEYE1", input_path=inRaster, DOBO="false", dem_path="", MS=uMS, PS=uPS, Pan=uPan)
                self.serviceExists = True
                return(1)
            if sensorName == "SPOT5":
                arcpy.Spot6(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_%s" % sensorName, input_path=inRaster, DOBO="false", dem_path="", MS=False, PS=False, Pan=True)
                self.serviceExists = True
                return(1)
            if sensorName == "SPOT6":
                arcpy.Spot6(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_%s" % sensorName, input_path=inRaster, DOBO="false", dem_path="", MS=uMS, PS=uPS, Pan=uPan)
                self.serviceExists = True
                return(1)
            if sensorName == "SPOT7":
                arcpy.Spot7(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_SPOT7", input_path=inRaster, DOBO="false", dem_path="", MS=uMS, PS=uPS, Pan=uPan)
                self.serviceExists = True
                return(1)
            if sensorName == "WV01":
                arcpy.WorldView1(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_WV01", input_path=inRaster, DOBO="false", dem_path="", MS=uMS, PS=uPS, Pan=uPan)
                self.serviceExists = True
                return(1)
            if sensorName == "WV02":
                print "uploading WV02"
                arcpy.WorldView2(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_WV02", input_path=inRaster, DOBO="false", dem_path="", MS=uMS, PS=uPS, Pan=uPan)
                self.serviceExists = True
                print "uploaded WV02"
                return(1)
            if sensorName == "WV03":
                arcpy.WorldView3(in_worksapce=outputWorkspace, in_mosaicdataset_name="S_WV03", input_path=inRaster, DOBO="false", dem_path="", MS=uMS, PS=uPS, Pan=uPan)
                self.serviceExists = True
                return(1)
            self.serviceExists = False
        except Exception, e:
            raise e.message
            
    def checkStatus(self):
        self.sourceExists = False
        self.serviceExists = False
        self.serviceOID = ''    
        self.sourceService = ''   
        self.rgbOID = ''
        try:
            with open(self.logFile,'r') as dest_f:
                data_iter = csv.reader(dest_f)  
                for data in data_iter:
                    if data[1] == self.sensor and data[0].strip() == os.path.basename(self.zipFile):

                        self.sourceExists = data[2].strip()
                        self.serviceExists = data[4].strip()
                        self.serviceOID = data[6].strip()  
                        self.sourceService = data[7].strip() 
                        self.rgbOID = data[8].strip()
        except:
            fubar="FUBAR" #Shouldn't I do something here?
            
        if not self.sourceExists:
            self.checkForSource()
        if not self.serviceExists or (self.serviceOID == '' or self.sourceService == ''):
            self.checkForService()
    
    def finalProcessing(self):
        '''
        Two final processing steps: Update the sensor field for the S_RGB datasets and apply the 8-bit stretch
        '''
        if self.serviceOID == '':
            self.checkForService()
            
        if self.serviceOID == '':
            tPrint("Something failed with finalization")
            return(-1)
        
        if self.sourceService == "S_RGB":
            doNothing = "For Now"
            #Create a table view of the selected table, then select this specific row
            arcpy.MakeTableView_management(self.rgbCatalog, "FUBAR", "OBJECTID = %s" % self.serviceOID)
            arcpy.CalculateField_management("FUBAR", "SensorName", "%s" % self.sensor)
        
        self.updateStatus()

    def updateStatus(self):
        self.statusUpdate = ",".join([os.path.basename(self.zipFile), self.sensor, str(self.sourceExists), 
            self.sourceFolder, str(self.serviceExists), str(self.rasterCatalog), str(self.serviceOID),
            self.sourceService, str(self.rgbOID)])
    
    def __init__(self, zipLocation, sensor,     
                basePath = "//ddhprdcifs/DDH-PRD/ddhfiles/internal/imagerysource", 
                tempFolder="//ddhprdcifs/DDH-PRD/ddhfiles/internal/imagerysource/Logs/CatalogInventory",                
                gdalRoot = "C:/Python27/ArcGIS10.3/Lib/site-packages/osgeo",  
                arcToolbox_HighRes = "I:/ddhfiles/internal/imagerysource/Scripts/HighResolution/Tools/HighResolution.pyt",
                arcToolbox_orthos = "I:/ddhfiles/internal/imagerysource/Scripts/PreprocessedOrthos/Tools/Preprocess.pyt",
                logFile = "I:/ddhfiles/internal/imagerysource/Logs/CURRENT_Imagery_Log.csv",
                listOnly = False, verbose=True):
        self.arcToolbox_HighRes = arcToolbox_HighRes
        self.arcToolbox_orthos = arcToolbox_orthos
        self.gdalRoot = gdalRoot
        self.zipFile = zipLocation
        self.sensor = sensor
        self.logFile = logFile
        self.sourceFolder = "%s/Source/%s/%s" % (basePath, sensor, os.path.basename(self.zipFile).replace(".zip", ""))          
        self.ingestFolder = "%s/Ingest/%s/%s" % (basePath, sensor, os.path.basename(self.zipFile).replace(".zip", ""))          
        for f in [self.sourceFolder, self.ingestFolder]:
            if not os.path.exists(f):
                os.makedirs(f)
        self.inputGDB = "%s/FGDB/Production/Imagery_Services_DEV.gdb" % basePath
        self.gdbPath = "%s/S_%s" % (self.inputGDB, sensor)    
        self.rgbPath = "%s/S_RGB" % (self.inputGDB)   
        self.drgbPath = "%s/D_RGB" % (self.inputGDB)   
        self.tempFolder = tempFolder
        self.rasterCatalog = "%s/%s_%s_catalog.dbf" % (tempFolder, datetime.date.today().strftime("%Y_%m_%d"), sensor)
        self.rgbCatalog =    "%s/%s_%s_catalog.dbf" % (tempFolder, datetime.date.today().strftime("%Y_%m_%d"), "RGB")
        self.drgbRasterCatalog = "%s/%s_%s_catalog.dbf" % (tempFolder, datetime.date.today().strftime("%Y_%m_%d"), "D_RGB")
        self.checkStatus()      
        self.updateStatus()
  
class ImageryGDB(object):
    def __init__(self, basePath = "//ddhprdcifs/DDH-PRD/ddhfiles/internal/imagerysource"):
        self.inputGDB = "%s/FGDB/ImageryServices.gdb" % basePath
        #Get a list of S_*** feature classes
        arcpy.env.workspace = self.inputGDB
        self.sourceDatasets = arcpy.ListDatasets("S_*")
        self.derivedDatasets = arcpy.ListDatasets("D_*")
        self.satelliteNames = {"S_DEM":"Digital Elevation Model", "S_DRONE":"Drone Imagery", "S_DSM":"Digital Surface Model",
            "S_GEOEYE1":"GeoEye 1", "S_PLEIADES":"Pleiades", "S_PLEIADES_Pan":"Pleiades", "S_PLEIADES_PS":"Pleiades",
            "S_RAPIDEYE":"Rapideye", "S_RGB":"Unknown Source", "S_SPOT7":"Spot 7", "S_SPOT7_PS":"Spot 7", "S_SPOT7_Pan":"Spot 7",
            "S_SPOT5":"Spot 5", "S_SPOT6_Pan":"Spot 6", "S_SPOT6_PS":"Spot 6", "S_SPOT6":"Spot 6", 
            "S_WV01":"WorldView 1", "S_WV02":"WorldView 2", "S_WV02_PS":"WorldView 2", "S_WV02_Pan":"WorldView 2",
            "S_WV03":"WorldView 3", "S_WV04":"WorldView 4"}
        
    def updateFields(self, changeFolders=[]):
        curFolders = self.sourceDatasets
        if len(changeFolders) > 0:
            curFolders = changeFolders
            
        for service in curFolders:
            satName = self.satelliteNames[service]  
            try:
                arcpy.CalculateField_management(service, "SensorName", satName)                
                tPrint("Updated the Sensor Name in %s" % service)
            except:
                print ("Could not update Sensor Name for %s" % satName)
                #arcpy.AddField_management(service, "SensorName", "TEXT", field_length=20)
                #arcpy.CalculateField_management(service, "SensorName", satName)                
    
    def applyStretch(self, changeFolders=[], 
        template = r"\\ddhprdcifs\DDH-PRD\ddhfiles\internal\imagerysource\Scripts\ImageryObjects\Templates\mini_max_stretch_255Max.rft.xml"):
        curFolders = self.sourceDatasets
        if len(changeFolders) > 0:
            curFolders = changeFolders
        
        #Loop through the services
        for service in curFolders:
            # Loop through the rasters in the service
            with arcpy.da.SearchCursor(service, ['OBJECTID']) as rCursor:
                for row in rCursor:
                    curLayer = "%s_%s" % (os.path.basename(service), row[0])
                    arcpy.MakeRasterLayer_management(service, curLayer, "OBJECTID = %s" % row[0])
                    ###BEN### - I need to apply the stretch function here to the specific row in the mosaic, rather than to the entire mosaic
            '''
            try:
                arcpy.EditRasterFunction_management(service, "EDIT_MOSAIC_DATASET_ITEM", "INSERT", template)
                tPrint("Updated the Stretch Function for %s" % service)
            except:
                print("Could not update Stretch Function in %s" % service)
            '''                   
    def finalProcessing(self):
        #Synchronize D_RGB
        arcpy.SynchronizeMosaicDataset_management(in_mosaic_dataset="//ddhprdcifs/DDH-PRD/ddhfiles/internal/imagerysource/FGDB/ImageryServices.gdb/D_RGB", where_clause="", new_items="UPDATE_WITH_NEW_ITEMS", sync_only_stale="SYNC_STALE", update_cellsize_ranges="UPDATE_CELL_SIZES", update_boundary="UPDATE_BOUNDARY", update_overviews="NO_OVERVIEWS", build_pyramids="NO_PYRAMIDS", calculate_statistics="NO_STATISTICS", build_thumbnails="NO_THUMBNAILS", build_item_cache="NO_ITEM_CACHE", rebuild_raster="REBUILD_RASTER", update_fields="UPDATE_FIELDS", fields_to_update="AcquisitionDate;Best;CenterX;CenterY;CloudCover;Dataset_ID;GroupName;LeafStatus;Metadata_URL;OffNadir;ProductName;Raster;SatAzimuth;SatElevation;SensorName;Shape;Source;SunAzimuth;SunElevation;Tag;Year;Zip_Path;ZOrder", existing_items="UPDATE_EXISTING_ITEMS", broken_items="REMOVE_BROKEN_ITEMS", skip_existing_items="SKIP_EXISTING_ITEMS", refresh_aggregate_info="REFRESH_INFO")        
        #Calculate Statistics
        
        #Update Overviews
'''RETIRED
        finally:
            try:  
                curSensor = self.sensor
                if curSensor == "OTHER":
                    curSensor = "RGB"
                arcpy.createSourceMD(workspace=self.inputGDB, md_name="S_%s" % curSensor, 
                                spref="PROJCS['WGS_1984_Web_Mercator_Auxiliary_Sphere',GEOGCS['GCS_WGS_1984',DATUM['D_WGS_1984',SPHEROID['WGS_1984',6378137.0,298.257223563]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Mercator_Auxiliary_Sphere'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',0.0],PARAMETER['Standard_Parallel_1',0.0],PARAMETER['Auxiliary_Sphere_Type',0.0],UNIT['Meter',1.0]]", 
                                datapath=inRaster, bfootprint="Remove black edges around imagery", 
                                inYear="2000", leaf="On", sourceurl="", metadataurl="")   
                self.serviceExists = True
                return(1)
            except:
                self.serviceExists = False
                tPrint("The generic tool failed")
'''