import sys, os, inspect, glob, datetime, time, shutil
import timeit
start = timeit.default_timer()

cmd_folder = os.path.dirname(os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0])))
if cmd_folder not in sys.path:
    sys.path.insert(0, cmd_folder)

import imageryObject

baseFolder = r"\\ddhprdcifs\DDH-PRD\ddhfiles\internal"
inputFolder = r"\\ddhprdcifs\DDH-PRD\ddhfiles\internal\imagerysource\Vendor_Distributions"
outLogFile = r"\\ddhprdcifs\DDH-PRD\ddhfiles\internal\imagerysource\Logs\Imagery_Log_%s.csv" % datetime.date.today().strftime("%Y_%m_%d")
finalLogFile = r"\\ddhprdcifs\DDH-PRD\ddhfiles\internal\imagerysource\Logs\CURRENT_Imagery_Log.csv"
performUpload = False

log = open(outLogFile, 'wb')
log.write("Sensor,ZipFile,SourceExists,SourceLocation,ServiceExists,ServiceSource,ServiceSourceOID,SourceService\n")


'''prints the time along with the message'''
def tPrint(s):
    print"%s\t%s" % (time.strftime("%H:%M:%S"), s)

allFiles = []

for dirName, subdirList, fileList in os.walk(inputFolder):
    #Run through and copy all the files in the fileList
    sensor = os.path.basename(dirName)
    for f in fileList:        
        if f[-4:] == ".zip" and not sensor in ['SPOT5','DRONE','DEM','AERIAL']:
            allFiles.append([f, sensor])

#allFiles = [['055997346020_01.zip', 'WV02']]

for s in allFiles:
    inputZip = "%s/%s/%s" % (inputFolder, s[1], s[0])    
    xx = imageryObject.imageryZip(inputZip, s[1])       
    print xx.statusUpdate
    if xx.sourceExists == False:
        print "*****Extracting and Tiling*****"
        xx.extractandTile()
    #Get outlines of source imagery and create output GEOJSON
    if xx.sourceExists:
        xx.createImageOutlines()
    if xx.serviceExists == False and performUpload:
        print "*****Uploading Source*****"
        xx.uploadSourceData()
    
    #xx.finalProcessing()
    log.write(xx.statusUpdate)
    log.write("\n")
log.close()
shutil.copyfile(outLogFile, finalLogFile)

print ("Finsihed Log")
stop = timeit.default_timer()
print stop - start

#synchronize the derived RGB
#fileGDB = imageryObject.ImageryGDB()
#fileGDB.updateFields()
#fileGDB.applyStretch()
