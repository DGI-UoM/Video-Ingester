"""
Created on Oct. 12 2011

@author: Jason MacWilliams

@TODO: email integration:waiting on server
"""

import os, sys, pwd
import ConfigParser

class ConfigData:
    def __init__(self, saveFile="IngesterState.save"):
        self.cfgFile = "controller.cfg"
        self.saveFile = saveFile
        self.fedoraUrl = None
        self.fedoraNS = None
        self.fedoraUser = None
        self.fedoraPW = None
        self.solrUrl = None
        self.inDir = None
        self.outDir = None # might not need this
        self.outUrl = None
        self.targetUser = None
        self.hostCollectionName = None
        self.hostCollectionPid = None
        self.datastreams = []
        self.converters = {}

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
            self.outUrl = cfgp.get("Controller", "output_url")
            self.mailTo = cfgp.get("Controller", "mail_to").replace(",", " ")
            self.datastreams = cfgp.get("Controller", "datastreams").split(",")

        except ConfigParser.NoSectionError, nsx:
            print("Error while parsing config file: %s" % nsx)
            return False
        except ConfigParser.NoOptionError, nox:
            print("Error while parsing config file: %s" % nox)
            return False

        try:
            self.targetUser = pwd.getpwnam(cfgp.get("Controller", "target_user"))
        except KeyError, kx:
            print "Error trying to locate the given target user (is it misspelled?)"
            print "Permission updates will be skipped"
            self.targetUser = ("", None, None, None, None, None)

        try:
            for key1 in self.datastreams:
                for key2 in self.datastreams:
                    option = "%s2%s" % (key1, key2)
                    if cfgp.has_option("Commands", option):
                        self.converters[option] = cfgp.get("Commands", option)
        except ConfigParser.NoOptionError, nox:
            print("Error while parsing converter commands from config file")
            return False
        return True

    def writeSaveHeader(self):
        fp = open(self.saveFile, "w")

        fp.write("[Fedora]\n")
        fp.write("url=%s\n" % self.fedoraUrl)
        fp.write("namespace=%s\n" % self.fedoraNS)
        fp.write("username=%s\n" % self.fedoraUser)
        fp.write("password=%s\n" % self.fedoraPW)
        fp.write("host_collection_name=%s\n" % self.hostCollectionName)
        fp.write("host_collection_pid=%s\n" % self.hostCollectionPid)
        fp.write("aggregate_name=%s\n" % self.aggregateName)
        fp.write("aggregate_pid=%s\n" % self.aggregatePid)

        fp.write("\n[Solr]\n")
        fp.write("url=%s\n" % self.solrUrl)

        fp.write("\n[Controller]\n")
        fp.write("input_dir=%s\n" % self.inDir)
        fp.write("output_dir=%s\n" % self.outDir)
        fp.write("output_url=%s\n" % self.outUrl)
        fp.write("target_user=%s\n" % self.targetUser[0])
        fp.write("mail_to=%s\n" % self.mailTo.replace(" ", ","))
        fp.write("datastreams=%s\n" % ",".join(self.datastreams))

        fp.write("\n[Commands]\n")
        for k, v in self.converters.iteritems():
            fp.write("%s=%s\n" % (k, v.replace("%", "%%")))

        fp.flush()
        fp.close()

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
        print("output_url = %s" % self.outUrl)
        print("target_user=%s" % self.targetUser[0])
        print("mail_to = %s" % self.mailTo)
        print("datastreams = %s" % str(self.datastreams))
        print("\n[Commands]")
        for k, v in self.converters.iteritems():
            print("%s = %s" % (k, v))
        print("======================================================")

    def getConverterCommand(self, fr, to):
        key = "%s2%s" % (fr, to)
        if self.converters.has_key(key):
            return self.converters[key]
        return None

    def fileIsComplete(self, file):
        return os.path.isfile(file.replace(self.inDir, self.outDir))

    def getTargetUid(self):
        return self.targetUser[2]

    def getTargetGid(self):
        return self.targetUser[3]
