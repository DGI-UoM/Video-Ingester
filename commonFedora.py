from islandoraUtils import fileConverter as converter
from islandoraUtils import fileManipulator
from islandoraUtils import misc
from islandoraUtils import fedoraLib
from islandoraUtils.metadata import fedora_relationships # for RELS-EXT stuff
# fcrepo imports
from fcrepo.connection import Connection, FedoraConnectionException
from fcrepo.client import FedoraClient

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
        return None
    return f

""" ====== MANAGING FEDORA OBJECTS ====== """

def addCollectionToFedora(fedora, myLabel, myPid, parentPid="islandora:top", contentModel="islandora:collectionCModel"):
    # put in the collection object
    try:
        collection_object = fedora.getObject(myPid)
        print("Attempted to create already existing collection %s" % myPid)
        return collection_object
    except FedoraConnectionException, fcx:
        if not fcx.httpcode in [404]:
            raise fcx

    collection_object = fedora.createObject(myPid, label=myLabel)

    # collection policy
    fedoraLib.update_datastream(collection_object, u"COLLECTION_POLICY", "collection_policy.xml", label=u'COLLECTION_POLICY', mimeType=u'text/xml', controlGroup=u'X')

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

