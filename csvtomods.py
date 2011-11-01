import sys, os
import csv
from MODSFile import MODSFile
#import pdb

def csv2mods(csvFilename, outDir=None):
    reader = csv.reader(open(csvFilename, "rb"), delimiter=",", quotechar='"')

    if not outDir:
        pathbase = os.path.split(os.path.realpath(csvFilename))[0]

    headers = reader.next()

    for row in reader:
        mods = MODSFile()
        fileName = "temp.xml"
        for key, value in zip(headers, row):
            if not value:
                continue # ignore this one - next item
            k = key.lower()

            if "title" in k:
                mods.addTitle(value)
            elif "name" in k:
                mods.addName(value)
            elif "subject" in k:
                # maybe add support for delimited compound subject values?
                mods.addSubject([value], [], [])
            elif "type" in k:
                continue
            elif "format" in k:
                continue
            elif "contributor" in k:
                mods.addName(value, role="Contributor")
            elif "creator" in k:
                mods.addName(value, type="corporate", role="Creator")
            elif "description" in k:
                continue
            elif "extentoriginal" in k:
                continue
            elif "extent" in k:
                continue
            elif "identifier" in k:
                if not os.path.isdir(outDir):
                    os.mkdir(outDir)
                fileName = os.path.join(outDor, value + os.extsep + "xml")
                mods.addIdentifier(value)
            elif "temporal" in k:
                mods.addSubject([], [value], [])
            elif "classification" in k:
                mods.addClassification(value)
            elif "language" in k:
                mods.addLanguageByName(value)

        with open(fileName, "wb") as file:
            mods.writeToFile(file)

def main(argv):
    csv2mods("metadata_Anderson.csv")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
