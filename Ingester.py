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
from csvtomods import csv2mods
import pdb

from commonFedora import *
from ConfigData import *
import Mailer

GENERATE_ME = "*"
config = ConfigData()

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

    print("Create aggregate object %s with pid=%s" % (config.aggregateName, config.aggregatePid))
    coll = addCollectionToFedora(fedora, config.aggregateName, config.aggregatePid, parentPid=config.hostCollectionPid)
    #add a TN datastream to the aggregate
    tnPath = os.path.join(config.inDir, "collection_TN.jpg")
    fedoraLib.update_datastream(coll, "TN", tnPath, label=unicode(config.aggregateName+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))

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
            obj = addObjectToFedora(fedora, unicode(mod), fedora.getNextPID(config.fedoraNS), config.aggregatePid, cmodel)
        except FedoraConnectionException, fcx:
            print("Connection error while trying to add fedora object - the connection to fedora may be broken")
            continue

        #add a TN datastream to the object
        tnPath = os.path.join(config.inDir, "thumbnail.jpg")
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
    Mailer.sendEmail(config.mailTo, "Script was terminated with signal %d" % signum, "Ingester Error")
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
    optionp.add_option("-c", "--config-file", type="string", dest="configfile", default=config.cfgFile, help="Path of the configuration file.")
    optionp.add_option("-i", "--ignore-save", action="store_true", dest="ignore", default=False, help="Ignore saved script state files when launching.")
    (options, args) = optionp.parse_args()

    if not os.path.exists(options.configfile):
        print("Config file %s not found!" % (options.configfile))
        optionp.print_help()
        return 1

    # put the name of the config file in the master dictionary - in case its different
    config.cfgFile = options.configfile

    """ ====== BASE CONFIGURATION FILE ====== """
    # load configuration
    print("Loading configuration file %s" % options.configfile)
    if not config.parse(options.configfile):
        print("*** Error loading configuration file ***")
        return 2

    if not os.path.isdir(config.inDir):
        print("Please verify the source directory: %s" % config.inDir)
        return 3

    # check for the destination directory
    if config.outDir == "":
        print("** Output directory not specified - Setting output_dir to input_dir **")
        config.outDir = config.inDir

    """ ====== CHECK FOR AND LOAD PREVIOUS STATE ====== """
    saveFile = os.path.join(config.outDir, config.saveFile)
    # if we are forcing a new run, or we can't find the requested save file
    newRun = options.ignore or (not os.path.isfile(saveFile))
    if newRun:
        if os.path.isdir(config.outDir):
            print("Creating new configuration save state in file")
            # create new script state
            config.writeSaveHeader(saveFile)
    else:
        # setup a resume of operations
        print("Loading configuration from file %s" % saveFile)
        print("*** This will override the values in the default configuration file ***")
        config.parse(saveFile)

        # now we have just loaded new configuration data, so we need to check outDir again
        if config.outDir == "":
            print("** Output directory not specified - Setting output_dir to input_dir **")
            config.outDir = config.inDir

        # setup the destination directory
        if not os.path.isdir(config.outDir):
            print("Output directory does not exist - creating directory %s" % config.outDir)
            os.makedirs(config.outDir)
            print("Creating new configuration save state in file")

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
    config.printSettings()

    """ ====== FEDORA SETUP ====== """
    # connect to fedora
    fedora = connectToFedora(config.fedoraUrl, config.fedoraUser, config.fedoraPW)
    if not fedora:
        print("Error connecting to fedora instance at %s" % config.fedoraUrl)
        return 5

    print("Try to create host collection object (pid=%s)" % config.hostCollectionPid)
    collection = addCollectionToFedora(fedora, config.hostCollectionName, myPid=config.hostCollectionPid)

    print("+-Scanning for files to ingest")
    print(" +-Scanning folder: %s" % config.inDir)
    dsfolders = {}
    for d in config.datastreams:
        dsfolders[d] = GENERATE_ME # a placeholder that means "generate me!"

    foundIndex = False
    fileList = os.listdir(config.inDir)
    for file in fileList:
        fullDirectory = os.path.join(config.inDir, file)
        if os.path.isdir(fullDirectory):
            # check for datastream source
            if file in config.datastreams:
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
            modsDir = os.path.join(config.outDir, "mods")
            if not os.path.isdir(os.path.join(modsDir)):
                os.makedirs(modsDir, 0755) # makedirs is a more extensive version of mkdir - it will create the entire tree if neccessary
            csv2mods(os.path.join(config.inDir, file), modsDir)
            foundIndex = True
            continue
        else:
            print("  -Found unclassified File: %s" % file)

    # we process the mods **after** looping through the files so we have the entire directory
    # scanned - that means we have all information we need before we run
    numFiles = 0
    if foundIndex:
        print("+-Performing ingest")
        # now os.path.join(outDir, "mods") contains my mods files
        numFiles = processModsFolder(fedora, os.path.join(config.outDir, "mods"), dsfolders)

    Mailer.sendEmail(config.mailTo, "Script run complete: %d files ingested" % numFiles, "Ingester Info")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
