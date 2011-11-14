"""
Created on Oct. 12 2011

@author: Jason MacWilliams
@dependencies: lxml

@TODO: email integration:waiting on server
"""

import os
import ConfigParser

"""
class ConfigData:
    def __init__(self):
        self.LOG_FILE_NAME = "Ingester"
        self.cfgFile = "controller.cfg"
        self.saveFile = 'IngesterState.save"
        self.fedoraUrl = None
        self.fedoraNS = None
        self.fedoraUser = None
        self.fedoraPW = None
        self.solrUrl = None
        self.inDir = None
        self.outDir = None # might not need this
        self.hostCollectionName = None
        self.hostCollectionPid = None
        self.datastreams = []
        self.files = {}
"""

# a couple of constants - should be moved into config dictionary
config = { "cfgFile" : "controller.cfg",
           "logFile" : "Ingester",
           "saveFile" : "IngesterState.save"
         }

def loadConfigFile(configFile):
    """
    This function get all the configuration values for use by the script.  The values are all packed
    up into a dictionary instead of just floating free - should probably create a struct to hold all
    this data maybe?  Something that's not as hackable as a dictionary
    """
    # prep the config file for input
    cfgp = ConfigParser.SafeConfigParser(defaults={}, allow_no_value=False)
    cfgp.read(configFile)

    try:
        data = { "fedoraUrl" : cfgp.get("Fedora", "url"),
                 "fedoraNS" : unicode(cfgp.get("Fedora", "namespace")),
                 "fedoraUser" : cfgp.get("Fedora", "username"),
                 "fedoraPW" : cfgp.get("Fedora", "password"),
                 "solrUrl" : cfgp.get("Solr", "url"),
                 "inDir" : os.path.expanduser(cfgp.get("Controller", "input_dir")),
                 "outDir" : os.path.expanduser(cfgp.get("Controller", "output_dir")),
                 "mailTo" : cfgp.get("Controller", "mail_to").replace(",", " "),
                 "aggregateName" : unicode(cfgp.get("Controller", "aggregate_name")),
                 "aggregatePid" : unicode(cfgp.get("Controller", "aggregate_pid")),
                 "hostCollectionName" : unicode(cfgp.get("Controller", "host_collection_name")),
                 "hostCollectionPid" : unicode(cfgp.get("Controller", "host_collection_pid")),
                 "datastreams" : cfgp.get("Controller", "datastreams").split(","),
                 "files" : cfgp.options("Files")
               }
    except ConfigParser.NoSectionError, nsx:
        print "Error while parsing config file: %s" % nsx
        return None
    except ConfigParser.NoOptionError, nox:
        print "Error while parsing config file: %s" % nox
        return None

    return data

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
    cfgp.set("Controller", "input_dir", config["inDir"])
    cfgp.set("Controller", "output_dir", config["outDir"])
    cfgp.set("Sontroller", "mail_to", config["mailTo"].replace(" ", ","))
    cfgp.set("Controller", "aggregate_name", config["aggregateName"])
    cfgp.set("Controller", "aggregate_pid", config["aggregatePid"])
    cfgp.set("Controller", "host_collection_name", config["hostCollectionName"])
    cfgp.set("Controller", "host_collection_pid", config["hostCollectionPid"])
    cfgp.set("Controller", "datastreams", ",".join(config["datastreams"]))
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
    print("input_dir = %s" % config["inDir"])
    print("output_dir = %s" % config["outDir"])
    print("mail_to = %s" % config["mailTo"])
    print("aggregate_name = %s" % config["aggregateName"])
    print("aggregate_pid = %s" % config["aggregatePid"])
    print("host_collection_name = %s" % config["hostCollectionName"])
    print("host_collection_pid = %s" % config["hostCollectionPid"])
    print("datastreams = %s" % str(config["datastreams"]))
    print("======================================================")
