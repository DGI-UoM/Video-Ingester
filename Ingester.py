"""
Created on Oct. 12 2011

@author: Jason MacWilliams
@dependencies: lxml

@TODO: email integration:waiting on server
"""

# system imports
import logging
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
sys.path.append("~/virtualcode/repositories/islandora/fcrepo/build/lib")
sys.path.append("~/virtualcode/repositories/islandora/pyutils/build/lib")

# islandora imports
from islandoraUtils import fileConverter as converter
from islandoraUtils import fileManipulator
from islandoraUtils import misc
from islandoraUtils.metadata import fedora_relationships
# fcrepo imports
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient

CONFIG_FILE_NAME = "controller.cfg"
SAVE_FILE_NAME = "IngesterState.save"
LOG_FILE_NAME = "Ingester"

def loadConfigFile(configFile):
    """
    This function get all the configuration values for use by the script
    """
    # prep the config file for input
    config = ConfigParser.SafeConfigParser()
    config.read(configFile)

    data = { "fedoraUrl" : config.get("Fedora", "url"),
             "fedoraNS" : unicode(config.get("Fedora", "namespace")),
             "fedoraUser" : config.get("Fedora", "username"),
             "fedoraPW" : config.get("Fedora", "password"),
             "solrUrl" : config.get("Solr", "url"),
             "inDir" : os.path.expanduser(config.get("Controller", "inputDir")),
             "outDir" : os.path.expanduser(config.get("Controller", "outputDir"))
           }

    return data

def connectToFedora(url, user, pw):
    try:
        connection = Connection(url, username=user, password=pw)
    except Exception, ex:
        print("Exception while connecting to fedoraUrl")
        return None

    try:
        f = FedoraClient(connection)
    except Exception, ex:
        print("Exception while opening fedora client - is fedora running?")
        print(ex)
        return None
    return f

# try to handle an abrupt shutdown more cleanly
def shutdown_handler(signum, frame):
    # is there enough time to save the script state
    sys.exit(1)


""" ====== S C R I P T   S T A R T ====== """

def main():
    # register handlers so we properly disconnect and reconnect
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    # parse the passed in command line options
    optionp = OptionParser()
    optionp.add_option("-C", "--config-file", type = "string", dest = "configfile", default = CONFIG_FILE_NAME,
                  help = "Path of the configuration file.")

    (options, args) = optionp.parse_args()

    if not os.path.exists(options.configfile):
        print("Config file %s not found!" % (options.configfile))
        optionp.print_help()
        sys.exit(1)

    # load configuration
    config = loadConfigFile(options.configfile)
    if not config:
        print("Could not load configuration file: %s", options.configfile)
        sys.exit(2)

    """ ====== DIRECTORY STRUCTURE ====== """
    if not os.path.isdir(config["inDir"]):
        print("Please verify the source/destination directories in %s", options.configfile)
        sys.exit(3)

    # make sure there is a destination directory
    if not config.has_key("outDir") or config["outDir"] == "":
        config["outDir"] = os.path.join(config["inDir"], "islandora")

    # setup the destination directory
    if not os.path.isdir(config["outDir"]):
        os.mkdir(config["outDir"])

    # setup the log directory
    logDir = os.path.join(config["inDir"], "logs")
    if not os.path.isdir(logDir):
        os.mkdir(logDir)

    """ ====== LOGGER ====== """
    # internal log file
    logFile = os.path.join(logDir, LOG_FILE_NAME + "-" + time.strftime("%y_%m_%d_%h") + ".log")
    logging.basicConfig(filename=logFile, level=logging.DEBUG)
    logging.info("logger ready")

    """ ====== GLOBAL/ENVIRONMENT VARIABLES ====== """
    # declaration of a dictionary to avoid conditional declaration and syntax ambiguity in assignment/creation

    # add cli,imageMagick to the path and hope for the best [remove these on production server]
    os.environ["PATH"]=os.environ["PATH"]+":/usr/local/ABBYY/FREngine-Linux-i686-9.0.0.126675/Samples/Samples/CommandLineInterface"
    os.environ["PATH"]=os.environ["PATH"]+":/usr/local/Linux-x86-64"
    os.environ["PATH"]=os.environ["PATH"]+":/usr/local/Exif"
    os.environ["PATH"]="/usr/local/bin:"+os.environ["PATH"]#need to prepend this one for precedence over pre-existing convert command

    #perl script location
    marc2mods = os.path.join(os.getcwd(), "marc2mods.pl")

    """ ====== FEDORA SETUP ====== """
    # connect to fedora
    fedora = connectToFedora(config["fedoraUrl"], config["fedoraUser"], config["fedoraPW"])
    if not fedora:
        logging.error("Error connecting to fedora instance at %s", config["fedoraUrl"])
        sys.exit(4)

    """ ====== CHECK FOR AND LOAD PREVIOUS STATE ====== """
    # path for script's save state
    saveFile = os.path.join(logDir, SAVE_FILE_NAME)
    #handle a resume of operations if necessary
    if os.path.isfile(saveFile):
        resumePastOperations()

    # get all subdirectories from sourceDir
    sourceDirList = []
    for p in os.listdir(config["inDir"]):
        if os.path.isdir(p):
            sourceDirList.append(p)

    logging.info(">Searching for items to ingest")
    #loop through those dirs
    for dir in sourceDirList:
        inDir = os.path.join(config["inDir"], dir)
        # outDir is a mirror of the folder structure inside sourceDir
        outDir = os.path.join(config["outDir"], dir)

        logging.info("> Searching folder: " + inDir)
        #loop through those files checking for a marc binary
        MARC_Check = False
        for file in os.listdir(currentDir):
            baseName, fileExt = os.path.splitext(file)
            # check for a marc/mrc file
            logging.info(">  File: %s%s" % (baseName, fileExt))
            # check for the "definition file"
            if fileExt in [".marc", ".mrc"]:
                MARC_Check = True
                logging.info("   MARC file found")
                if not os.path.isdir(outDir):
                    os.mkdir(outDir)
                #run Jonathan's perl script here and record the new location of the mods file
                os.chdir(currentDir)
                perlCall = ["perl", marc2mods, os.path.join(inDir, file)]
                subprocess.call(perlCall)
                modsFilePath = os.path.join(inDir, "mods_book.xml")
                #add book obj to fedora
                addBookToFedora()
                fileList.remove(file)
                break
        #if there was a marc file found file run tif=>ocr, tif=>jp2
        # if there was more than one, we only use the first one found
        if MARC_Check:
            performOps()


"""
def navigate(inDir, outDir, files):
    for file in files:
        #if it is past 7:30am stop the script and record current state
        currentTime = time.localtime()
        if (currentTime[3] + currentTime[4]/60.0 > 7.5):
            #record state [current directory and files checked already]
            outFile = open(resumeFilePath, 'w')
            outFile.write(inDir + '\n')
            outFile.write(outDir + '\n')
            outFile.write(bookPid + '\n')
            outFile.write(str(pagesDict) + '\n')
            for fileToWrite in files:
                outFile.write(fileToWrite + '\n')
            outFile.close()
            #exit script
            logging.warning("The ingest has stopped for day time activities")
            sys.exit()

        fileName, fileExt = os.path.splitext(file)
        if fileExt in [".tif", ".tiff"]:
            logging.info("Performing operations on file:" + file)
            addBookPageToFedora(os.path.join(inDir, file), outDir)

        #remove file that has been operated on so it will not be operated on again on a script resume
        if files.count(file)!=0:#fixes a bug where created files were throwing errors
            files.remove(file)
    #remove base dir
    createBookPDF(outDir)
    shutil.rmtree(inDir)
    shutil.rmtree(outDir)
    return True
"""

if __name__ == "__main__":
    sys.exit(main())
