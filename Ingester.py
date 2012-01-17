#!/usr/bin/python
"""
Created on Oct. 12 2011

@author: Jason MacWilliams
"""

import sys, os, time, signal, subprocess, atexit
from optparse import OptionParser
from csvtomods import csv2mods
import pdb

from commonFedora import *
from ConfigData import *
import Mailer

DRYRUN = False

GENERATE_ME = "*"
config = ConfigData()
message = Mailer.EmailMessage()

# these are the options to the ffmpeg call for the target output format
# use "ffmpeg -i %s %s %s" % (input_file, ffmpeg_switches[target_format], target_file)
# ALSO: this list is a lot smaller/simpler than originally thought
# so turning these entries into config options could be useful
ffmpeg_switches = {
    "mp3" : "-acodec libmp3lame -ab 256k",
    "ogg" : "-acodec libvorbis -aq 60",
    "aac" : "-acodec libfaac",
    "ac3" : "-acodec ac3",
    "wav" : "",
    "mp4" : "-f mp4 -vcodec copy -acodec copy"
}

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

    coll = addCollectionToFedora(fedora, config.aggregateName, config.aggregatePid, parentPid=config.hostCollectionPid)
    #add a TN datastream to the aggregate
    tnPath = os.path.join(config.inDir, "collection_TN.jpg")
    if not DRYRUN:
        fedoraLib.update_datastream(coll, "TN", tnPath, label=unicode(config.aggregateName+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))

    print("Prepare destination for file copy")
    for k in dsFolders.keys():
        dsDir = os.path.join(config.outDir, k)
        if not os.path.isdir(dsDir):
            os.makedirs(dsDir, 0755) # makedirs is a more extensive version of mkdir - it will create the entire tree if neccessary
            user = config.getTargetUid()
            if user:
                os.chown(dsDir, user, config.getTargetGid())

    # the keys from dsFolders tells me what types of files are being ingested
    cmodel = ""
    for k, v in dsFolders.iteritems():
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

        if not DRYRUN:
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
        #print dsFolders
        dsFileBase = os.path.splitext(mod)[0]
        masterDS = ""
        for k,v in dsFolders.iteritems():
            filename = dsFileBase + os.extsep + k
            if os.path.isfile(os.path.join(v, filename)) and v != GENERATE_ME:
                masterDS = k
                print("Set master datastream = %s (%s)" % (masterDS, dsFolders[masterDS]))
                break;
        if masterDS == "":
            # could not find master datastream
            print("Could not find master datastream")
            message.addLine("Ingesting object %s, masterDS not found" % dsFileBase)
            continue
        else:
            message.addLine("Ingesting object %s, masterDS = %s" % (dsFileBase, dsFolders[masterDS]))

        # loop through dsFolders and search for more objects to datastreamify
        print("Scan for additional datastreams...")
        for k, v in dsFolders.iteritems():
            dsFile = dsFileBase + os.extsep + k
            path = os.path.join(v, dsFile)
            target = ""
            print("Check file: %s" % path)
            if config.fileIsComplete(path):
                print("Config settings report that file is already complete")
                continue
            if not os.path.isfile(path):#v == GENERATE_ME:
                masterFileName = dsFileBase + os.extsep + masterDS
                masterFile = os.path.join(dsFolders[masterDS], masterFileName)
                print("Generate file...")
                # best way to generate these files?
                target = os.path.join(config.outDir, k, os.path.basename(path))
                cmd = config.getConverterCommand(masterDS, k)
                if cmd:
                    cmd = cmd % (masterFile, target)
                    print "Using user supplied command: %s" % cmd
                else:
                    cmd = "ffmpeg -i '%s' %s '%s'" % (masterFile, ffmpeg_switches[k], target)
                    print "Using fallback command: %s" % cmd
                if DRYRUN:
                    print("Generate file %s from master %s using cmd=[%s]" % (target, masterFile, cmd))
                else:
                    # exec <cmd>
                    # using shell=True can cause security problems here, if one of the files
                    # contains some malicious code as part of its filename, things will go sour
                    # really fast
                    if "'" in masterFile or "'" in target:
                        print("ERROR - one of the ingest operations is about to cause a problem:")
                        print("Command: <%s>" % cmd)
                        print("Please sanitize your file names and paths")
                        message.addLine("Critical ingest error")
                        message.addLine("Possibly malformed ingest command detected: %s\nMight be malicious code" % cmd)
                        return -1
                    else:
                        message.addLine("Converting file %s to create %s" % (masterFile, target))
                        subprocess.call(cmd, shell=True)
                # we're not going to record this file operation in the save file since the source
                # file was generated and not supplied
            else:
                # just copy the file here
                if (config.inDir != config.outDir):
                    target = os.path.join(config.outDir, k, os.path.basename(path))
                    if DRYRUN:
                        print("cp %s %s" % (path, target))
                    else:
                        print("Copy file %s into place" % path)
                        message.addLine("Copying file %s to %s" % (path, target))
                        shutil.copy(path, os.path.split(target)[0])
            if os.path.exists(target):
                print("Ingest file...")
                user = config.getTargetUid()
                if user:
                    os.chown(target, user, config.getTargetGid())
                os.chmod(target, 0755)
                if not DRYRUN:
                    fedoraLib.update_datastream(obj, k.upper(), target.replace(config.outDir, config.outUrl), label=dsFile, mimeType=misc.getMimeType(k), controlGroup='R')

    return count

# try to handle an abrupt shutdown more cleanly
def shutdown_handler(signum, frame):
    # is there enough time to save the script state, do we even have to?
    print("Script terminating with signal %d" % signum)
    if config.mailTo:
        Mailer.sendEmail(config.mailTo, "Script was terminated with signal %d" % signum, "Ingester Error")
    # we might also have to remove the last object as it may be corrupt
    # need to look into how an interrupt can interfere with shutil.copy, os.chown, and ffmpeg
    sys.exit(1)

def sendReport():
    message.send()
    print("Email report sent")

""" ====== M A I N ====== """
def main(argv):
    global DRYRUN

    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    optionp = OptionParser()
    optionp.add_option("-c", "--config-file", type="string", dest="configfile", default=config.cfgFile, help="Path of the configuration file.")
    optionp.add_option("-i", "--ignore-save", action="store_true", dest="ignore", default=False, help="Ignore saved script state files when launching.")
    optionp.add_option("-d", "--dry-run", action="store_true", dest="dryrun", default=False, help="Perform a dry run of the script: make folders, but don't move/convert files, and don't create any fedora objects.")
    (options, args) = optionp.parse_args()

    DRYRUN = options.dryrun

    if DRYRUN:
        print("Launch Ingester in SKELETON mode...")
    else:
        print("Launch Ingester...")
    print("argv=%s" % str(argv))

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
    config.saveFile = os.path.join(config.outDir, config.saveFile)
    # if we are forcing a new run, or we can't find the requested save file
    newRun = options.ignore or (not os.path.isfile(config.saveFile))
    if newRun:
        if not os.path.isdir(config.outDir):
            os.makedirs(config.outDir, 0755)
            user = config.getTargetUid()
            if user:
                os.chown(config.outDir, user, config.getTargetGid())
        print("Creating new configuration save state in file %s" % config.saveFile)
        # create new script state
        config.writeSaveHeader()
    else:
        # setup a resume of operations
        print("Loading configuration from file %s" % config.saveFile)
        print("*** This will override the values in the default configuration file ***")
        if not config.parse(config.saveFile):
            return 4

        # now we have just loaded new configuration data, so we need to check outDir again
        if config.outDir == "":
            print("** Output directory not specified - Setting output_dir to input_dir **")
            config.outDir = config.inDir

        # setup the destination directory
        if not os.path.isdir(config.outDir):
            print("Output directory does not exist - creating directory %s" % config.outDir)
            os.makedirs(config.outDir, 0755)
            user = config.getTargetUid()
            if user:
                os.chown(config.outDir, user, config.getTargetGid())
            print("Creating new configuration save state in file")
            config.writeSaveHeader()

    for addr in config.mailTo.split(" "):
        message.addAddress(addr)
    message.setSubject("%s report" % argv[0])
    atexit.register(sendReport)

    if DRYRUN:
        message.addLine("Running in SKELETON mode")

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
        message.addLine("Error connecting to fedora instance at %s" % config.fedoraUrl)
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
            if not os.path.isdir(modsDir):
                os.makedirs(modsDir, 0755) # makedirs is a more extensive version of mkdir - it will create the entire tree if neccessary
                user = config.getTargetUid()
                if user:
                    os.chown(modsDir, user, config.getTargetGid())
            try:
                csv2mods(os.path.join(config.inDir, file), modsDir)
            except:
                print("Failed to run csv2mods - skipping")
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
    else:
        print("+-Index not found - terminating")
        return 6

    if numFiles < 0:
        return nunmFiles
    message.addLine("Script run complete: %d files ingested" % numFiles)
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
