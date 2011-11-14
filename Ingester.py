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
from lxml import etree
from optparse import OptionParser
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
import pdb

from ConfigData import *
import Mailer

GENERATE_ME = "*"

def connectToFedora(url, user, pw):
    """
    Attempt to create a connection to fedora using the supplied username and password.  If the
    connection succeeds, return the connected fedora client, otherwise return None.  The calling
    function should terminate if None is received.
    """
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

def addCollectionToFedora(fedora, myLabel, myPid, parentPid="islandora:top", contentModel="islandora:collectionCModel"):
    # put in the collection object
    collection_policy = u'<collection_policy xmlns="http://www.islandora.ca" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" name="" xsi:schemaLocation="http://www.islandora.ca http://syn.lib.umanitoba.ca/collection_policy.xsd"><content_models><content_model dsid="ISLANDORACM" name="Book Content Model" namespace="islandora:1" pid="islandora:bookCModel"></content_model></content_models><search_terms></search_terms><staging_area></staging_area><relationship>isMemberOf</relationship></collection_policy>'
    try:
        collection_object = fedora.getObject(myPid)
        print("Attempted to create already existing collection %s" % myPid)
        return collection_object
    except FedoraConnectionException, fcx:
        if not fcx.httpcode in [404]:
            raise fcx

    collection_object = fedora.createObject(myPid, label=myLabel)

    # collection policy
    try:
        collection_object.addDataStream(u'COLLECTION_POLICY', collection_policy, label=u'COLLECTION_POLICY',
        mimeType=u'text/xml', controlGroup=u'X', # X=inline xml
        logMessage=u'Added basic COLLECTION_POLICY data.')
        print("Added COLLECTION_POLICY datastream to %s" % myPid)
    except FedoraConnectionException, fcx:
        print("Error adding COLLECTION_POLICY datastream to %s" % myPid)
        raise fcx

    #add relationships
    collection_object_RELS_EXT=fedora_relationships.rels_ext(collection_object, [fedora_relationships.rels_namespace('fedora-model', 'info:fedora/fedora-system:def/model#')])
    collection_object_RELS_EXT.addRelationship(fedora_relationships.rels_predicate('fedora-model', 'hasModel'), contentModel)
    collection_object_RELS_EXT.addRelationship('isMemberOfCollection', parentPid)
    loop = True
    while loop:
        loop = False
        try:
            collection_object_RELS_EXT.update()
        except FedoraConnectionException as fedoraEXL:
            if str(fedoraEXL.body).find("is currently being modified by another thread") != -1:
                loop = True
                print("Trouble (thread lock) updating obj(%s) RELS-EXT - retrying." % myPid)
            else:
                print("Error updating obj(%s) RELS-EXT" % myPid)
    return collection_object

def addObjectToFedora(fedora, myLabel, myPid, parentPid, contentModel):
    # check for invalid parentPid, invalid contentModel
    # create the fedora object
    # validate the pid
    try:
        obj = fedora.getObject(myPid)
        print("Attempted to create already existing object %s" % myPid)
        return obj
    except FedoraConnectionException, fcx:
        if fcx.httpcode not in [404]:
            raise fcx

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
                print("Trouble (thread lock) updating obj(%s) RELS-EXT - retrying." % myPid)
            else:
                print("Error updating obj(%s) RELS-EXT" % myPid)

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

""" ====== INGEST FILES IN A FOLDER ====== """
def processModsFolder(fedora, folder, dsFolders):
    """
    Create a bunch of fedora objects (1 for each mods file in @folder) and ingest the mods file as
    a datastream (called MODS).  Then loop through the datastream source folders (@dsFolders) and
    find files with matching names - ingest these as datastreams to the master object.
    """
    # first make sure folder is a valid folder
    if not os.path.isdir(folder):
        return 0

    print("Create aggregate object %s with pid=%s" % (config["aggregateName"], config["aggregatePid"]))
    coll = addCollectionToFedora(fedora, config["aggregateName"], config["aggregatePid"], parentPid=config["hostCollectionPid"])
    #add a TN datastream to the aggregate
    tnPath = os.path.join(config["inDir"], "collection_TN.jpg")
    fedoraLib.update_datastream(coll, "TN", tnPath, label=unicode(config["aggregateName"]+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))

    # the keys from dsFolders tells me what types of files are being ingested
    cmodel = ""
    for k,v in dsFolders.iteritems():
        if k:
            m = misc.getMimeType(k).split("/")[0]
            if m in [ "audio", "video" ]:
                cmodel = "islandora:sp-%sCModel" % m
                break

    print("Selecting \"%s\" Content Model from datastreams list" % cmodel)

    print("Search for mods files in %s" % folder)
    print("Datastream sources: %s" % str(dsFolders))
    count = 0
    modsFiles = []
    for file in os.listdir(folder):
        fileName, fileExt = os.path.splitext(os.path.basename(file))
        if fileExt in [ ".xml", ".mods" ]:
            # found a mods file
            count = count + 1
            modsFiles.append(file)
    print("Process %d records for ingest" % count)
    for idx, mod in zip(range(len(modsFiles)), modsFiles):
        print("Ingesting object %d of %d: %s" % (idx+1, count, mod))
        try:
            obj = addObjectToFedora(fedora, unicode(mod), fedora.getNextPID(config["fedoraNS"]), config["aggregatePid"], cmodel)
        except FedoraConnectionException, fcx:
            print("Connection error while trying to add fedora object - the connection to fedora may be broken")
            continue

        #add a TN datastream to the object
        tnPath = os.path.join(config["inDir"], "thumbnail.jpg")
        fedoraLib.update_datastream(obj, "TN", tnPath, label=unicode(mod+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))

        # apply the mods datastream
        fedoraLib.update_datastream(obj, "MODS", os.path.join(folder, mod), label=mod, mimeType=misc.getMimeType("xml"), controlGroup='M')

        # find the "master datastream" - the one to generate all missing ones from
        # the master is the first one in the list that exists
        masterDS = ""
        for k in dsFolders.keys():
            if k != "":
                masterDS = k
                break;
        # now we know what file to use as a master generator, but what function?

        # loop through dsFolders and search for more objects to datastreamify
        print("Scan for additional datastreams...")
        for k, v in dsFolders.iteritems():
            if v == GENERATE_ME:
                # best way to generate these files?
                # this block could get ugly
                continue
            dsFile = os.path.splitext(mod)[0] + os.extsep + k
            path = os.path.join(v, dsFile)
            print("Check file %s" % path)
            if os.path.exists(path):
                print("Found datastream!")
                fedoraLib.update_datastream(obj, k.upper(), path, label=dsFile, mimeType=misc.getMimeType(k), controlGroup='R')

    return count

# try to handle an abrupt shutdown more cleanly
def shutdown_handler(signum, frame):
    # is there enough time to save the script state, do we even have to?
    print("Script terminating with signal %d" % signum)
    Mailer.sendEmail(config["mailTo"], "Script was terminated with signal %d" % signum, "Ingester Error")
    sys.stdout.close() # this will close the "save state" file cleanly
    sys.exit(1)

""" ====== M A I N ====== """
def main(argv):
    print("Launch Ingester...")
    print("argv=%s" % str(argv))

    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    optionp = OptionParser()
    optionp.add_option("-c", "--config-file", type="string", dest="configfile", default=config["cfgFile"], help="Path of the configuration file.")
    optionp.add_option("-i", "--ignore-save", action="store_true", dest="ignore", default=False, help="Ignore saved script state files when launching.")
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

    if not os.path.isdir(config["inDir"]):
        print("Please verify the source directory: %s" % config["inDir"])
        return 3

    # check for the destination directory
    if config["outDir"] == "":
        print("** Output directory not specified - Setting output_dir to input_dir **")
        config["outDir"] = config["inDir"]

    """ ====== CHECK FOR AND LOAD PREVIOUS STATE ====== """
    if options.ignore:
        print("Received 'ignore save' option, skipping save state loading")
    elif os.path.isdir(config["outDir"]):
        # path for script's save state
        saveFile = os.path.join(config["outDir"], config["saveFile"])
        # handle a resume of operations if necessary
        print("Search for save file %s" % saveFile)
        if os.path.isfile(saveFile):
            print("Loading configuration from previously saved state")
            print("*** This will override the values in the default configuration file ***")
            loadConfigFile(saveFile)

            # now we have just loaded new configuration data, so we need to check outDir again
            if config["outDir"] == "":
                print("** Output directory not specified - Setting output_dir to input_dir **")
                config["outDir"] = config["inDir"]
        else:
            print("Creating new configuration save state in file")
            # create new script state
            writeSaveHeader(saveFile)

    """ ====== DIRECTORY STRUCTURE ====== """
    # setup the destination directory
    if not os.path.isdir(config["outDir"]):
        print("Output directory does not exist - creating directory %s" % config["outDir"])
        os.mkdir(config["outDir"])
        print("Creating new configuration save state in file")
        # create new script state
        saveFile = os.path.join(config["outDir"], config["saveFile"])
        writeSaveHeader(saveFile)

    # setup the log directory
    logDir = os.path.join(config["outDir"], "logs")
    try:
        if not os.path.isdir(logDir):
            os.mkdir(logDir)
    except:
        print("Failed to create logs directory: probably an OS Error: check permissions and directory tree structure")
        return 4

    """ ====== LOGGER ====== """
    # internal log file
    logFile = os.path.join(logDir, "%s-%s.log" % (config["logFile"], time.strftime("%y-%m-%d_%H:%M:%S")))
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

    # display to the user what settings are being used for this run
    printConfigSettings()

    """ ====== FEDORA SETUP ====== """
    # connect to fedora
    fedora = connectToFedora(config["fedoraUrl"], config["fedoraUser"], config["fedoraPW"])
    if not fedora:
        print("Error connecting to fedora instance at %s" % config["fedoraUrl"])
        return 5

    print("Create host collection object (pid=%s)" % config["hostCollectionPid"])
    collection = addCollectionToFedora(fedora, config["hostCollectionName"], myPid=config["hostCollectionPid"])

    print("+-Scanning for files to ingest")
    print(" +-Scanning folder: %s" % config["inDir"])
    dsfolders = {}
    for d in config["datastreams"]:
        dsfolders[d] = GENERATE_ME # a placeholder that means "generate me!"

    foundMods = False
    fileList = os.listdir(config["inDir"])
    for file in fileList:
        fullDirectory = os.path.join(config["inDir"], file)
        if os.path.isdir(fullDirectory):
            # check for datastream source
            if file in config["datastreams"]:
                print("  -Found a datastream source folder: %s" % file)
                dsfolders[file] = fullDirectory
            else:
                print("  -Skipping directory %s" % file)
            continue
        if file[0] == ".":
            print("  -Skipping file %s" % file)
            continue
        baseName, fileExt = os.path.splitext(file)
        if fileExt in [ ".log" ]:
            print("  -Skipping log file %s" % file)
            continue
        # check for the "index" file
        if fileExt == ".csv":
            # launch csv2mods
            print("  -Found index(csv) file %s" % file)
            modsDir = os.path.join(config["outDir"], "mods")
            if not os.path.isdir(os.path.join(modsDir)):
                os.makedirs(modsDir, 0755) # makedirs is a more extensive version of mkdir - it will create the entire tree if neccessary
            csv2mods(os.path.join(config["inDir"], file), modsDir)
            foundMods = True
            continue
        else:
            print("  -Found unclassified File: %s" % file)

    # we process the mods **after** looping through the files so we have the entire directory
    # scanned - that means we have all information we need before we run
    numfiles = 0
    if foundMods:
        print("+-Performing ingest")
        # now os.path.join(outDir, "mods") contains my mods files
        numFiles = processModsFolder(fedora, os.path.join(config["outDir"], "mods"), dsfolders)

    Mailer.sendEmail(config["mailTo"], "Script run complete: %d files ingested" % numFiles, "Ingester Info")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
