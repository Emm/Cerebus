import math
import random

from twisted.spread import flavors

class Filter(flavors.Copyable, flavors.RemoteCopy):
    status = 'created'
    pass

class XmlFilter(Filter):
    def run(self):
        print "xml_filter"
        result = 0
        for i in range(random.randint(1000000, 3000000)):
            angle = math.radians(random.randint(0, 45))
            result += math.tanh(angle)/math.cosh(angle)
        return "%s, result: %.2f" % (self, result)

class XslFilter(Filter):
    def run(self):
        print "xsl_filter"
        result = 0
        for i in range(random.randint(1000000, 3000000)):
            angle = math.radians(random.randint(0, 45))
            result += math.tanh(angle)/math.cosh(angle)
        return "%s, result: %.2f" % (self, result)

flavors.setUnjellyableForClass(XmlFilter, XmlFilter)
flavors.setUnjellyableForClass(XslFilter, XslFilter)
