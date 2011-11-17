from lxml import etree
import marccountries

class MODSFile:
    HTML_NS = 'http://www.w2.org/TR/xhtml1/DTD/xhtml1-strict.dtd'
    XML_NS = 'http://www.loc.gov/mods/v3'
    XLINK = 'http://www.w3.org/1999/xlink'
    XSI = 'http://www.w3.org/2001/XMLSchema-instance'
    SCHEMA_NS = 'http://www.loc.gov/standards/mods/v3/mods-3-0.xsd'
    NSMap = {
        None            : XML_NS,
        'xlink'         : XLINK,
        'xsi'           : XSI
    }

    def __init__(self):
        """
        maybe parameterize the version?  that could cause problems, we'll have to check the version
        before adding certain elements so we don't add invalid tags/attributes
        """
        self.root = etree.Element('mods', { "version" : "3.0", "{%s}schemaLocation" % self.XSI : self.SCHEMA_NS }, nsmap=self.NSMap)
        self.originInfo = None

    def addTitle(self, title, subtitle=None, type=None):
        """
        Add a title field to this mods file
        @param title The title
        @param subtitle The subtitle (optional)
        @param type The type (optional)
        @todo Add <nonSort> tag options
        @todo Add support for the <partName> tag (might be a <relateditem> thing only)
        @todo Add support for language attribute in <title> tag (might also be in <subtitle>)
        """
        dict = {}
        if type:
            dict["type"] = type
        child = etree.SubElement(self.root, "titleInfo", dict)
        etree.SubElement(child, "title").text = title
        if subtitle:
            etree.SubElement(child, "subtitle").text = subtitle
        return self

    def addSubject(self, topic, temporal, geographic, authority="lcsh"):
        to = topic
        te = temporal
        ge = geographic
        if not isinstance(topic, list):
            to = [topic]
        if not isinstance(temporal, list):
            te = [temporal]
        if not isinstance(geographic, list):
            ge = [geographic]
        return self._addSubject(to, te, ge, authority)

    def _addSubject(self, topics, temporal, geographic, authority="lcsh"):
        child = etree.SubElement(self.root, "subject", { "authority" : authority })
        for t in topics:
            etree.SubElement(child, "topic").text = t
        for t in temporal:
            etree.SubElement(child, "temporal").text = t
        for g in geographic:
            etree.SubElement(child, "geographic").text = g
        return self

    def addLanguageByCode(self, langCode):
        """
        Add a language to this object
        @param langCode The iso639-2b code for the language
        """
        child = etree.SubElement(self.root, "language")
        etree.SubElement(child, "languageTerm", { "authority" : "iso639-2b", "type" : "code" }).text = langCode
        return self

    def addLanguageByName(self, langName):
        """
        Add a language to this object
        @param langName The human readable name of the language
        """
        child = etree.SubElement(self.root, "language")
        etree.SubElement(child, "languageTerm").text = langName
        return self

    """
    add support for compound corporate names, can this fit in with these functions already here?
    corporate name is the same as adding a family name, only instead of "family" and "given" its
    "primary" and "secondary" but theres no types on the <namepart> tag
    """

    def addName(self, name1, name2=None, type="personal", dates=None, role=None, roleType="text"):
        """
        Add a name (author/creator/etc.)
        @note Should probably be extracted into 2 or 3 different functions
        @param name1 The first name of the person/company to add.  If name2 is None then name1 is the full name
        @param name2 The second name of the person/company to add.  May be None
        @param type The type of association between the person and the object: {personal, corporate, conference, family}
        @param role The role of the person with respect to the object
        @param roleType The type of the role entered, either of {code, text}
        """
        child = etree.SubElement(self.root, "name", { "type" : type })
        if type == "personal":
            if name2:
                etree.SubElement(child, "namepart", { "type" : "family" }).text = name1
                etree.SubElement(child, "namepart", { "type" : "given" }).text = name2
            else:
                etree.SubElement(child, "namepart").text = name1
        else:
            etree.SubElement(child, "namepart").text = name1
            if name2:
                etree.SubElement(child, "namepart").text = name2

        if dates:
            etree.SubElement(child, "namepart", { "type" : "date" }).text = dates
        if role:
            child = etree.SubElement(child, "role")
            etree.SubElement(child, "roleTerm", { "type" : roleType }).text = role
        return self

    def addRecordInfo(self, contentSource, creationDate, changeDate, identifier):
        child = etree.SubElement(self.root, "recordInfo")
        etree.SubElement(child, "recordContentSource").text = contentSource
        etree.SubElement(child, "recordCreationDate", { "encoding" : "marc" }).text = creationDate
        etree.SubElement(child, "recordChangeDate", { "encoding" : "iso8601" }).text = changeDate
        etree.SubElement(child, "recordIdentifier").text = identifier
        return self

    def addOriginInfo(self, place, placeCode, publisher, date, issuance, frequency=None):
        """
        Add origin info to this mods file
        @place The location (city/province/etc.)
        @placeCode The marccountry code
        @publisher The publisher's name
        @date The date this object was issued
        @issuance Valid values are "continuing", "monographic", "single unit", "multipart monograph",
                  "serial", "integrating resource"
        @note origin info can have multiple dates attached (may be limited to 1,2,3)
        @note It look like most of the fields can also be left out - need to support this
        """
        child = etree.SubElement(self.root, "originInfo")
        child1 = etree.SubElement(child, "place")
        etree.SubElement(child1, "placeterm", { "authority" : "marccountry", "type" : "code" }).text = placeCode
        child1 = etree.SubElement(child, "place")
        etree.SubElement(child1, "placeterm", { "type" : "text" }).text = place
        etree.SubElement(child, "publisher").text = publisher
        etree.SubElement(child, "dateIssued").text = date
        etree.SubElement(child, "issuance").text = issuance
        if frequency:
            etree.SubElement(child, "frequency").text = frequency
        return self

    def addClassification(self, classification, authority=None, edition=None):
        dict = {}
        if authority:
            dict["authority"] = authority
        if edition:
            dict["edition"] = edition
        etree.SubElement(self.root, "classification", dict).text = classification
        return self

    def addPhysLocation(self, location):
        child = etree.SubElement(self.root, "location")
        etree.SubElement(child, "physicalLocation").text = location
        return self

    def addURLLocation(self, location):
        child = etree.SubElement(self.root, "location")
        etree.SubElement(child, "url").text = location
        return self

    def addPhysicalDescription(self, form, extent):
        """
        Add a physical description of the object
        @param form The form this object was originally in
        @param extent The extent of this object inside its parent object (article in a book)
        @note The definition of "extent" might be wrong
        @note There can be multiple <form> tags with different attributes
        @note form might be absent
        """
        child = etree.SubElement(self.root, "physicalDescription")
        if form:
            etree.SubElement(child, "form", { "authority" : "marcform" }).text = form
        if extent:
            etree.SubElement(child, extent).text = extent
        return self

    def addTypeOfResource(self, resType):
        etree.SubElement(self.root, "typeOfResource").text = resType
        return self

    def addIdentifier(self, identifier):
        etree.SubElement(self.root, "identifier").text = identifier
        return self

    def addTargetAudience(self, audience):
        etree.SubElement(self.root, "targetAudience").text = audience
        return self

    def addGenre(self, genre, authority=None):
        dict = {}
        if authority:
            dict["authority"] = authority
        etree.SubElement(self.root, "genre", dict).text = genre
        return self

    def addTableOfContents(self, contents):
        etree.SubElement(self.root, "tableOfContents").text = contents
        return self

    def addNote(self, note, type=None):
        dict = {}
        if type:
            dict["type"] = type
        etree.SubElement(self.root, "note", dict).text = note
        return self

    def addAbstract(self, abstract):
        etree.SubElement(self.root, "abstract").text = abstract
        return self

    def addAccessCondition(self, condition, type):
        """
        Add an access condition to this object
        @param condition The condition text to add
        @param type The type of access condition.  Valid values are "restriction on access" and "use and reproduction"
        """
        etree.SubElement(self.root, "accessCondition", { "type" : type }).text = condition
        return self

    def _checkOriginInfo(self):
        if not self.originInfo:
            self.originInfo = etree.SubElement(self.root, "originInfo")

    # origin info
    def addOriginInfoPlaceByName(self, placeName):
        self._checkOriginInfo()
        child = etree.SubElement(self.originInfo, "place")
        etree.SubElement(child, "placeTerm", { "type" : "text" }).text = placeName
        code = marccountries.findCodeByCountry(placeName)
        if code:
            child = etree.SubElement(self.originInfo, "place")
            etree.SubElement(child, "placeTerm", { "authority" : "marccountry", "type" : "code" }).text = code
        return self.originInfo

    def addOriginInfoPlaceByCode(self, placeCode):
        self._checkOriginInfo()
        child = etree.SubElement(self.originInfo, "place")
        etree.SubElement(child, "placeTerm", { "authority": "marccountry", "type" : "code" }).text = placeCode
        country = marccountries.findCountryByCode(placeCode)
        if country:
            child = etree.SubElement(self.originInfo, "place")
            etree.SubElement(child, "placeTerm", { "type" : "text" }).text = country
        return self.originInfo

    def addOriginInfoPublisher(self, publisher):
        self._checkOriginInfo()
        etree.SubElement(self.originInfo, "publisher").text = publisher
        return self.originInfo

    def addOriginInfoDateIssued(self, date, encoding="marc"):
        self._checkOriginInfo()
        etree.SubElement(self.originInfo, "dateIssued", { "encoding" : encoding }).text = date
        return self.originInfo

    def addOriginInfoIssuance(self, issuance):
        self._checkOriginInfo()
        etree.SubElement(self.originInfo, "issuance").text = issuance
        return self.originInfo

    def writeToFile(self, file):
        """
        print the completed xml tree to a file
        @param file The file to print the tree to
        """
        # file should be an open file
        file.write(etree.tostring(self.root, pretty_print=True))
