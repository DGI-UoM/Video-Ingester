from types import *
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
        return FedoraClient(connection)
    except Exception, ex:
        print("Exception while opening fedora client")
        print("Check if fedora is running and your login information is correct")
    return None

""" ====== MANAGING FEDORA OBJECTS ====== """

def createRelsExt(childObject, parentPid, contentModel, extraNamespaces={}, extraRelationships={}):
    """
    Create the RELS-EXT relationships between childObject and object:parentPid
    We set the default namespace for our interconnections, then apply the content model, and make
    childObject a member of the object:parentPid collection.  If object:parentPid doesn't have the
    collection content model then hilarity could ensue...
    @param childObject The FedoraObject to attach the RELS-EXT to.
    @param parentPid The pid of the parent to assign to childObject.
    @param contentModel The contentmode to give to childObject.
    @param extraNamespaces Any extra namespaces to put in the RELS-EXT data.
    @param extraRelationsips Any additional relationship values to assign to childObject.  By default
           the object gets: hasModel:contentModel and isMemberOfCollection:parentPid
    """

    nsmap = [ fedora_relationships.rels_namespace('fedora', 'info:fedora/fedora-system:def/relations-external#'),
              fedora_relationships.rels_namespace('fedora-model', 'info:fedora/fedora-system:def/model#')
             ]
    if extraNamespaces and type(extraNamespaces) is DictType:
        for k, v in extraNamespaces.iteritems():
            nsmap.append(fedora_relationships.rels_namespace(k, v))

    #add relationships
    rels_ext=fedora_relationships.rels_ext(childObject, nsmap, 'fedora')

    rels_ext.addRelationship(fedora_relationships.rels_predicate('fedora-model', 'hasModel'), [contentModel, "pid"])
    rels_ext.addRelationship(fedora_relationships.rels_predicate('fedora', 'isMemberOfCollection'), [parentPid, "pid"])
    if extraRelationships and type(extraRelationships) is DictType:
        for k, v in extraRelationships.iteritems():
            rels_ext.addRelationship(k, [v, "literal"])

    loop = True
    while loop:
        loop = False
        try:
            rels_ext.update()
        except FedoraConnectionException, fedoraEXL:
            if str(fedoraEXL.body).find("is currently being modified by another thread") != -1:
                loop = True
                print("Trouble (thread lock) updating obj(%s) RELS-EXT - retrying." % childObject.pid)
            else:
                print("Error updating obj(%s) RELS-EXT" % childObject.pid)
    return rels_ext

def addCollectionToFedora(fedora, myLabel, myPid, parentPid="islandora:top", contentModel="islandora:collectionCModel", tnUrl=None):
    """
    Add a collection (not an object) to fedora
    @param fedora The fedora instance to add the collection to
    @param myLabel The label to apply to the collection object object
    @param myPid The pid of the collection to try and create, if the pid is already a valid object/collection, then return that object instead
    @parentPid [optional] The parent object to nest this one under
    @contentModel [optional] The content model to attach to this collection object
    @tnUrl [optional] The url of an image to use as the thumbnail
    """

    print("Attempt to create collection '%s' with pid=%s" % (myLabel, myPid))
    # validate the pid
    try:
        collection_object = fedora.getObject(myPid)
        print("Attempted to create already existing collection %s" % myPid)
        return collection_object
    except FedoraConnectionException, fcx:
        if not fcx.httpcode in [404]:
            raise fcx

    collection_object = fedora.createObject(myPid, label=myLabel)

    # this is the biggest difference between objects and collections - a collection policy
    # collection policy
    fedoraLib.update_datastream(collection_object, u"COLLECTION_POLICY", "collection_policy.xml", label=u'COLLECTION_POLICY', mimeType=u'text/xml', controlGroup=u'X')

    # thumnail, if one is supplied
    if tnUrl:
        # possibly check if tnUrl is a valid jpg
        #add a TN datastream to the object after creating it from the book cover
        fedoraLib.update_datastream(collection_object, 'TN', tnUrl, label=myLabel+'_TN.jpg', mimeType='image/jpeg')

    # rels-ext relations
    collection_relsext = createRelsExt(collection_object, parentPid, contentModel)

    return collection_object

def addObjectToFedora(fedora, myLabel, myPid, parentPid, contentModel, tnUrl=None, extraNamespaces={}, extraRelationships={}):
    """
    Add an object (not a collection) to fedora
    @param fedora The fedora instance to add the object to
    @param myLabel The label to apply to the object object
    @param myPid The pid of the object to try and create, if the pid is already a valid object/collection, then return that object instead
    @parentPid The parent object to nest this one under
    @contentModel The content model to attach to this object
    @tnUrl [optional] The url of an image to use as the thumbnail
    """
    # check for invalid parentPid, invalid contentModel

    print("Attempt to create object '%s' with pid=%s" % (myLabel, myPid))
    # validate the pid
    try:
        # try to create the fedora object
        obj = fedora.getObject(myPid)
        print("Attempted to create already existing object %s" % myPid)
        return obj
    except FedoraConnectionException, fcx:
        if fcx.httpcode not in [404]:
            # this will throw a bunch of exceptions - all of them to the tune of "cannot connect to fedora"
            raise fcx

    obj = fedora.createObject(myPid, label=myLabel)
    print("Object created")

    # thumbnail, if one is supplied
    if tnUrl:
        # possibly check if tnUrl is a valid jpg
        #add a TN datastream to the object after creating it from the book cover
        fedoraLib.update_datastream(obj, 'TN', tnUrl, label=myLabel+'_TN.jpg', mimeType='image/jpeg')

    # rels-ext relations
    obj_relsext = createRelsExt(obj, parentPid, contentModel, extraNamespaces=extraNamespaces, extraRelationships=extraRelationships)

    return obj

# this function is taken from the old book converter
# it might not be needed as these features may be automatic
def sendToSolr():
    """
    This is a helper function that creates and sends information to solr for ingest
    """

    """
    solrFile = os.path.join(os.path.dirname(config["modsFilePath"]), 'mods_book_solr.xml')
    converter.mods_to_solr(config["modsFilePath"], solrFile)
    solrFileHandle = open(solrFile, 'r')
    solrFileContent = solrFileHandle.read()
    solrFileContent = solrFileContent[solrFileContent.index('\n'):]
    curlCall = 'curl %s/update?commit=true -H "Content-Type: text/xml" --data-binary "%s"' % (config.solrUrl, solrFileContent)

    # be careful here, shell=True can cause problems
    r = subprocess.call(curlCall, shell=True)
    if r != 0:
        logging.error('Trouble currling with Solr power. Curl returned code: %d' % r)
    solrFileHandle.close()
    """
    return True
