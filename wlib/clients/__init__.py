__author__ = 'badpoet'

import pyhocon

conf = pyhocon.ConfigFactory.parse_file("resources/config.hocon")
def get_conf():
    return conf
