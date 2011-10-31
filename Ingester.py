"""
Created on Oct. 12 2011

@author: Jason MacWilliams
@dependencies: lxml

@TODO: email integration:waiting on server
"""

# system imports
import sys
import os
import time
#import subprocess
#import shutil
import signal
import ConfigParser
from lxml import etree
from optparse import OptionParser

# these two might not be neccessary
#sys.path.append("~/virtualcode/repositories/islandora/fcrepo/build/lib")
#sys.path.append("~/virtualcode/repositories/islandora/pyutils/build/lib")

# islandora imports
from islandoraUtils import fileConverter as converter
from islandoraUtils import fileManipulator
from islandoraUtils import misc
from islandoraUtils import fedoraLib
from islandoraUtils.metadata import fedora_relationships # for RELS-EXT stuff
# fcrepo imports
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient
from csvtomods import csv2mods

# a couple of constants - should be moved into config dictionary
LOG_FILE_NAME = "Ingester"
config = { "cfgFile" : "controller.cfg",
           "saveFile" : "IngesterState.save"
         }

class DualWriter:
    """
    A simple class designed to take anything written to sys.stdout and fork it to a file
    """
    def __init__(self, stdout, filename, filemode='a'):
        self.savedOut = sys.stdout
        self.stdout = stdout
        self.logfile = file(filename, filemode)
    def write(self, text):
        self.stdout.write(text)
        self.logfile.write(text)
    def close(self):
        self.stdout.close()
        self.logfile.close()
    def flush(self):
        self.stdout.flush()
        self.logfile.flush()
    def close(self):
        sys.stdout = self.savedOut # not sure if this is right
        self.logfile.close()

def loadConfigFile(configFile):
    """
    This function get all the configuration values for use by the script
    """
    # prep the config file for input
    cfgp = ConfigParser.RawConfigParser(defaults={}, allow_no_value=False)
    cfgp.read(configFile)

    try:
        data = { "fedoraUrl" : cfgp.get("Fedora", "url"),
                 "fedoraNS" : unicode(cfgp.get("Fedora", "namespace")),
                 "fedoraUser" : cfgp.get("Fedora", "username"),
                 "fedoraPW" : cfgp.get("Fedora", "password"),
                 "solrUrl" : cfgp.get("Solr", "url"),
                 "inDir" : os.path.expanduser(cfgp.get("Controller", "input_dir")),
                 "outDir" : os.path.expanduser(cfgp.get("Controller", "output_dir")),
                 "fileTypes" : cfgp.get("Controller", "file_types").split(","),
                 "hostCollectionName" : unicode(cfgp.get("Controller", "host_collection_name")),
                 "hostCollectionPid" : unicode(cfgp.get("Controller", "host_collection_pid")),
                 "files" : cfgp.options("Files")
               }
    except ConfigParser.NoSectionError, nsx:
        print "Error while parsing config file: %s" % nsx
        return None
    except ConfigParser.NoOptionError, nox:
        print "Error while parsing config file: %s" % nox
        return None

    return data

def connectToFedora(url, user, pw):
    try:
        connection = Connection(url, username=user, password=pw)
    except Exception, ex:
        print("Error while connecting to fedoraUrl: %s", ex.message)
        return None

    try:
        f = FedoraClient(connection)
    except Exception, ex:
        print("Exception while opening fedora client")
        print("Check if fedora is running and your login information is correct")
        print(ex.message)
        return None
    return f

""" ====== MANAGING FEDORA OBJECTS ====== """

def addObjectToFedora(fedora, myLabel, myPid=None, parentPid="islandora:top", contentModel="islandora:collectionCModel"):
    # check for invalid parentPid, invalid contentModel
    # create the fedora object
    if myPid:
        # validate the pid
        try:
            obj = fedora.getObject(myPid)
            print ("Attempted to create already existing object %s" % myPid)
            return obj
        except FedoraConnectionException, fcx:
            if fcx.httpcode not in [404]:
                raise fcx
    else:
        myPid = fedora.getNextPID(config["fedoraNS"])

    print("Fedora object %s does not exit - create it" % myPid)
    #myLabel = unicode(os.path.basename(os.path.dirname(modsFilePath)))
    obj = fedora.createObject(myPid, label=myLabel)

    #configure rels ext
    objRelsExt = fedora_relationships.rels_ext(obj, fedora_relationships.rels_namespace("fedora-model", "info:fedora/fedora-system:def/model#"))
    objRelsExt.addRelationship(fedora_relationships.rels_predicate("fedora-model", "hasModel"), contentModel)
    objRelsExt.addRelationship("isMemberOfCollection", parentPid)

    loop = True
    while loop:
        loop = False
        try:
            objRelsExt.update()
        except FedoraConnectionException as fedoraEXL:
            if str(fedoraEXL.body).find("is currently being modified by another thread") != -1:
                loop = True
                print("Trouble (thread lock) updating obj RELS-EXT: " + myPid + " retrying.")
            else:
                print("Error updating obj RELS-EXT: " + myPid)

    """
    #add the book pid to modsFile
    parser = etree.XMLParser(remove_blank_text=True)
    xmlFile = etree.parse(modsFilePath, parser)
    xmlFileRoot = xmlFile.getroot()
    modsElem = etree.Element("{http://www.loc.gov/mods/v3}identifier", type="pid")
    modsElem.text = bookPid
    xmlFileRoot.append(modsElem)
    xmlFile.write(modsFilePath)

    #add mods datastream
    obj.update_datastream('MODS', modsFilePath, label=modsFilePath, mimeType='text/xml', controlGroupl='X')

    #add a TN datastream to the object after creating it from the book cover
    tnPath = os.path.join(os.path.dirname(modsFilePath), (myLabel + '_TN.jpg'))
    converter.tif_to_jpg(os.path.join(os.path.dirname(modsFilePath), '0001_a_front_cover.tif'), tnPath, 'TN')
    tnUrl = open(tnPath)
    obj.update_datastream('TN', tnPath, label=myLabel+'_TN.jpg', mimeType='image/jpeg')

    #index the object in solr
    sendToSolr()
    """
    return obj

# try to handle an abrupt shutdown more cleanly
def shutdown_handler(signum, frame):
    # is there enough time to save the script state, do we even have to?
    print("Script terminating with signal %d" % signum)
    sys.stdout.close() # this will close the "save state" file cleanly
    sys.exit(1)

""" ====== SAVING AND LOADING SCRIPT STATES ====== """

# write a record to the script save state
def writeSaveRecord(saveFile, record):
    file = open(saveFile, 'a')
    file.write(record + '\n')
    file.flush()
    file.close()
    # close after every write so if the script stops (for any reason), the save state will be intact

def writeSaveHeader(saveFile):
    fp = open(saveFile, "w")
    # prep the config file for input
    cfgp = ConfigParser.SafeConfigParser(defaults={}, allow_no_value=True)
    cfgp.add_section("Fedora")
    cfgp.set("Fedora", "url", config["fedoraUrl"])
    cfgp.set("Fedora", "namespace", config["fedoraNS"])
    cfgp.set("Fedora", "username", config["fedoraUser"])
    cfgp.set("Fedora", "password", config["fedoraPW"])
    cfgp.add_section("Solr")
    cfgp.set("Solr", "url", config["solrUrl"])
    cfgp.add_section("Controller")
    cfgp.set("Controller", "inputDir", config["inDir"])
    cfgp.set("Controller", "outputDir", config["outDir"])
    cfgp.set("Controller", "host_collection_name", config["hostCollectionName"])
    cfgp.set("Controller", "host_collection_pid", config["hostCollectionPid"])
    cfgp.add_section("Files")
    # just the section header so no values(files) are written here
    cfgp.write(fp)
    fp.close()

def printConfigSettings():
    print("======================================================")
    print("=== Configuration data ===")
    print("\n[Fedora]")
    print("url = %s" % config["fedoraUrl"])
    print("namespace = %s" % config["fedoraNS"])
    print("username = %s" % config["fedoraUser"])
    print("password = %s" % config["fedoraPW"])
    print("\n[Solr]")
    print("url = %s" % config["solrUrl"])
    print("\n[Controller]")
    print("inputDir = %s" % config["inDir"])
    print("outputDir = %s" % config["outDir"])
    print("host_collection_name = %s" % config["hostCollectionName"])
    print("host_collection_pid = %s" % config["hostCollectionPid"])
    print("======================================================")

def ingestFromModsFile(fedora, modsFile, inDir, outDir, fileList):
    if not os.path.isdir(outDir):
        os.mkdir(outDir)

    #add collection object to fedora
    #addObjectToFedora(fedora, "", parentPid)
    fileList.remove(file)
    return True

""" ====== M A I N ====== """
def main(argv):
    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    optionp = OptionParser()
    optionp.add_option("-C", "--config-file", type="string", dest="configfile", default=config["cfgFile"], help="Path of the configuration file.")
    (options, args) = optionp.parse_args()

    if not os.path.exists(options.configfile):
        print("Config file %s not found!" % (options.configfile))
        optionp.print_help()
        return 1

    # put the name of the config file in the master dictionary - in case its different
    config["cfgFile"] = options.configfile

    """ ====== BASE CONFIGURATION FILE ====== """
    # load configuration
    print("Loading configuration file %s" % options.configfile)
    c = loadConfigFile(options.configfile)
    if not c:
        print("*** Error loading configuration file ***")
        return 2
    config.update(c)

    """ ====== CHECK FOR AND LOAD PREVIOUS STATE ====== """
    # path for script's save state
    saveFile = os.path.join(config["outDir"], config["saveFile"])
    # handle a resume of operations if necessary
    print("Search for save file %s" % saveFile)
    if os.path.isfile(saveFile):
        print("Loading configuration from previously saved state")
        print("*** This will override the values in the default configuration file ***")
        loadConfigFile(saveFile)
    else:
        print("Creating new configuration save state")
        # create new script state
        writeSaveHeader(saveFile)

    """ ====== DIRECTORY STRUCTURE ====== """
    if not os.path.isdir(config["inDir"]):
        print("Please verify the source/destination directories in %s", options.configfile)
        return 3

    # make sure there is a destination directory
    if not config.has_key("outDir") or config["outDir"] == "":
        config["outDir"] = os.path.join(config["inDir"], "islandora")

    # setup the destination directory
    if not os.path.isdir(config["outDir"]):
        os.mkdir(config["outDir"])

    # setup the log directory
    logDir = os.path.join(config["outDir"], "logs")
    if not os.path.isdir(logDir):
        os.mkdir(logDir)

    """ ====== LOGGER ====== """
    # internal log file
    logFile = os.path.join(logDir, LOG_FILE_NAME + "-" + time.strftime("%y-%m-%d_%H:%M:%S") + ".log")
    sys.stdout = DualWriter(sys.stdout, logFile)
    print("logger ready")

    """ ====== ENVIRONMENT VARIABLES ====== """
    # add cli,imageMagick to the path and hope for the best [remove these on production server]
    #os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/ABBYY/FREngine-Linux-i686-9.0.0.126675/Samples/Samples/CommandLineInterface"
    #os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/Linux-x86-64"
    #os.environ["PATH"] = os.environ["PATH"] + ":/usr/local/Exif"
    # not sure about this - syn doesn't have convert in this path
    convertPath = "/usr/local/bin"
    if not os.environ["PATH"].startswith(convertPath):
        os.environ["PATH"] = convertPath + ":" + os.environ["PATH"]#need to prepend this one for precedence over pre-existing convert command

    """ ====== FEDORA SETUP ====== """
    # connect to fedora
    fedora = connectToFedora(config["fedoraUrl"], config["fedoraUser"], config["fedoraPW"])
    if not fedora:
        print("Error connecting to fedora instance at %s" % config["fedoraUrl"])
        return 4

    # get all subdirectories from sourceDir
    sourceDirList = []
    for p in os.listdir(config["inDir"]):
        if os.path.isdir(os.path.join(config["inDir"], p)):
            sourceDirList.append(p)

    # display to the user what settings are being used for this run
    printConfigSettings()

    print("Create host collection object (pid=%s)" % config["hostCollectionPid"])
    collection = addObjectToFedora(fedora, config["hostCollectionPid"], myPid=config["hostCollectionPid"], parentPid="islandora:top")

    print("+-Searching for items to ingest")
    print(" -Root folder = %s" % config["inDir"])
    print(" -Subfolders = %s" % str(sourceDirList))

    #loop through those dirs
    for dir in sourceDirList:
        inDir = os.path.join(config["inDir"], dir)
        # outDir is a mirror of the folder structure inside sourceDir
        outDir = os.path.join(config["outDir"], dir)

        print(" +-Searching folder: " + inDir)
        #loop through those files checking for a marc binary
        modsFile = ""
        fileList = os.listdir(inDir)
        for file in fileList:
            if os.path.isdir(os.path.join(inDir, file)):
                print("  -Skipping directory %s" % file)
                continue
            if file[0] == ".":
                print("  -Skipping file %s" % file)
                continue
            baseName, fileExt = os.path.splitext(file)
            if fileExt in [ ".log" ]:
                print("  -Skipping log file %s" % file)
                continue
            if fileExt == ".csv":
                # launch csv2mods
                print("  -Found index(csv) file %s" % file)
                if not os.path.isdir(outDir):
                    os.makedirs(outDir, 0755) # makedirs is a more extensive version of mkdir - it will create the entire tree if neccessary
                csv2mods(os.path.join(inDir, file), outDir)
                continue
            # check for the "definition file"
            elif fileExt == ".xml":
                modsFile = file
                print("  -Found MODS file %s" % modsFile)
                ingestFromModsFile(fedora, modsFile, inDir, outDir, fileList)
                break
            else:
                print("  -Found unclassified File: %s" % file)

        # if there was more than one, we only use the first one found
        if modsFile:
            ingestFromModsFiles(fedora, modsFile)

    sys.stdout.close()
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
