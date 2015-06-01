__author__ = 'badpoet'

import pymongo

def connection(conf):
    db = pymongo.Connection(
        host=conf.get_string('db.mongo.host'),
        port=conf.get_int('db.mongo.port'),
    )[conf.get_string('db.mongo.db')]
    # db.authenticate(
    #     conf.get_string('db.mongo.user'),
    #     conf.get_string('db.mongo.pw'),
    # )
    return db
