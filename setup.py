__author__ = 'badpoet'

import MySQLdb
import pymongo
import codecs
import pyhocon


conf = pyhocon.ConfigFactory.parse_file("resources/config.hocon")


