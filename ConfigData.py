"""
Created on Oct. 12 2011

@author: Jason MacWilliams

@TODO: email integration:waiting on server
"""

import os, sys
import ConfigParser

class ConfigData:
    def __init__(self):
        self.cfgFile = "controller.cfg"
        self.saveFile = "IngesterState.save"
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

    def parse(self, configFile):
        self.cfgFile = configFile
        # prep the config file for input
        if sys.version_info >= (2, 7):
            cfgp = ConfigParser.SafeConfigParser(allow_no_value=True)
        else:
            cfgp = ConfigParser.SafeConfigParser()
        cfgp.read(configFile)

        try:
            self.fedoraUrl = cfgp.get("Fedora", "url")
            self.fedoraNS = unicode(cfgp.get("Fedora", "namespace"))
            self.fedoraUser = cfgp.get("Fedora", "username")
            self.fedoraPW = cfgp.get("Fedora", "password")
            self.hostCollectionName = unicode(cfgp.get("Fedora", "host_collection_name"))
            self.hostCollectionPid = unicode(cfgp.get("Fedora", "host_collection_pid"))
            self.aggregateName = unicode(cfgp.get("Fedora", "aggregate_name"))
            self.aggregatePid = unicode(cfgp.get("Fedora", "aggregate_pid"))
            self.solrUrl = cfgp.get("Solr", "url")
            self.inDir = os.path.expanduser(cfgp.get("Controller", "input_dir"))
            self.outDir = os.path.expanduser(cfgp.get("Controller", "output_dir"))
            self.mailTo = cfgp.get("Controller", "mail_to").replace(",", " ")
            self.datastreams = cfgp.get("Controller", "datastreams").split(",")
            self.files = cfgp.options("Files")
        except ConfigParser.NoSectionError, nsx:
            print "Error while parsing config file: %s" % nsx
            return False
        except ConfigParser.NoOptionError, nox:
            print "Error while parsing config file: %s" % nox
            return False
        return True

    def writeSaveHeader(self, saveFile):
        fp = open(saveFile, "w")
        # prep the config file for input
        if sys.version_info >= (2, 7):
            cfgp = ConfigParser.SafeConfigParser(allow_no_value=True)
        else:
            cfgp = ConfigParser.SafeConfigParser()

        cfgp.add_section("Fedora")
        cfgp.set("Fedora", "url", self.fedoraUrl)
        cfgp.set("Fedora", "namespace", self.fedoraNS)
        cfgp.set("Fedora", "username", self.fedoraUser)
        cfgp.set("Fedora", "password", self.fedoraPW)
        cfgp.set("Fedora", "host_collection_name", self.hostCollectionName)
        cfgp.set("Fedora", "host_collection_pid", self.hostCollectionPid)
        cfgp.set("Fedora", "aggregate_name", self.aggregateName)
        cfgp.set("Fedora", "aggregate_pid", self.aggregatePid)
        cfgp.add_section("Solr")
        cfgp.set("Solr", "url", self.solrUrl)
        cfgp.add_section("Controller")
        cfgp.set("Controller", "input_dir", self.inDir)
        cfgp.set("Controller", "output_dir", self.outDir)
        cfgp.set("Controller", "mail_to", self.mailTo.replace(" ", ","))
        cfgp.set("Controller", "datastreams", ",".join(self.datastreams))
        cfgp.add_section("Files")
        # just the section header so no values(files) are written here
        cfgp.write(fp)
        fp.close()

    # write a record to the script save state
    def writeSaveRecord(self, saveFile, record):
        self.files.append(record)
        file = open(saveFile, 'a')
        file.write(record + '\n')
        file.flush()
        file.close()
        # close after every write so if the script stops (for any reason), the save state will be intact

    def printSettings(self):
        print("======================================================")
        print("=== Configuration data ===")
        print("\n[Fedora]")
        print("url = %s" % self.fedoraUrl)
        print("namespace = %s" % self.fedoraNS)
        print("username = %s" % self.fedoraUser)
        print("password = %s" % self.fedoraPW)
        print("host_collection_name = %s" % self.hostCollectionName)
        print("host_collection_pid = %s" % self.hostCollectionPid)
        print("aggregate_name = %s" % self.aggregateName)
        print("aggregate_pid = %s" % self.aggregatePid)
        print("\n[Solr]")
        print("url = %s" % self.solrUrl)
        print("\n[Controller]")
        print("input_dir = %s" % self.inDir)
        print("output_dir = %s" % self.outDir)
        print("mail_to = %s" % self.mailTo)
        print("datastreams = %s" % str(self.datastreams))
        print("======================================================")
