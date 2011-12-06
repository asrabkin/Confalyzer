from xml.sax.handler import ContentHandler 
import xml.sax


class HconfSAX(ContentHandler):
    
    def __init__(self):
        self.opts = {}  #a set of key-value pairs
        self.descriptions = {}
        self.name = ""
        self.value = ""
        self.description = ""
        self.curElem = ""
        
    def startElement(self,name, attrs):
        self.curElem = name
    
    def endElement(self,name):
        self.curElem = ""
        if name == "property":
            self.opts[str(self.name)] = str(self.value)
            self.descriptions[str(self.name)] = str(self.description)

            self.name = ""
            self.value = ""
            self.description = ""

    
    def characters(self,chars):
        if self.curElem == "name":
            self.name += chars
        elif self.curElem == "value":
            self.value += chars
        elif self.curElem == "description":
            self.description += chars

def getOptsFromXML(hconfFile):
    """Given the contents of a hadoop conf file as a string,
    add all key-value pairs to dictionary opts.
    Keys and values will be strings."""
    handler = HconfSAX()
    xml.sax.parseString(hconfFile, handler)
    return (handler.opts, handler.descriptions)

def getOptsFromFile(hconfFile):
    """Arg can be either a file handle or filename"""
    handler = HconfSAX()
    xml.sax.parse(hconfFile, handler)
    return (handler.opts, handler.descriptions)
