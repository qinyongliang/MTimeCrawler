#!/bin/python3
#-*- coding: UTF-8 -*-
import logging
from pyArango.connection import *
from pyArango.graph import *
import random

conn = Connection(arangoURL="http://localhost:8529",
                  username="root", password="123456")
db = None

collections = ['movie', 'person',  'leaguer']
edges = ['leaguer_edge', 'relation_edge', 'personate_edge']

if(not conn.hasDatabase('mtime')):
    db = conn.createDatabase(name="mtime")
else:
    db = conn['mtime']


# 创建集合和边
for collection in collections:
    if(not db.hasCollection(collection)):
        db.createCollection(name=collection)

#创建一个图
class MovieRelation(Graph):
    _edgeDefinitions = [EdgeDefinition("leaguer_edge", fromCollections=["movie"], toCollections=["person"]),
                        EdgeDefinition("relation_edge", fromCollections=["person"], toCollections=["person"]),
                        EdgeDefinition("personate_edge", fromCollections=["person"], toCollections=["movie"])]
    _orphanedCollections = []


for edge in edges:
    if(not db.hasCollection(edge)):
        db.createCollection(name=edge, className='Edges')

if(not db.hasGraph('MovieRelation')):
    db.createGraph("MovieRelation")

cache = {}


def getConnect(collection):
    global cache
    if(collection not in cache.keys()):
        cache[collection] = db[collection]
    return cache[collection]


def save(name, data, _key=None, type='doc', force=True):
    doc = None
    if(type == 'doc'):
        doc = getConnect(name).createDocument(data)
    elif type == 'edge':
        doc = getConnect(name).createEdge(data)
    if(_key != None):
        if(force):
            try:
                getConnect(name)[_key].delete()
            except:
                pass
        doc._key = str(_key)
    try:
        doc.save()
    except:
        pass


def saveMovieInfo(movie):
    getConnect('movie').createDocument(movie).save()


def savePersonInfo(person):
    getConnect('person').createDocument(person).save()


def next(collection):
    aql = '''for doc in {0}
                              filter doc.raw == true
                              sort doc.{1} desc
                              limit {2},{2}
                              return doc
                              '''.format(collection, 'rating' if collection == 'movie' else 'movieCount', random.randint(1, 50))
    queryResult = db.AQLQuery(aql, rawResults=True, skip=int())
    for todo in queryResult:
        return todo
    return None


def have(collection, _key):
    try:
        data = getConnect(collection)[str(_key)]
        if(data['raw']):
            return False
        return True
    except:
        return False


def getdb():
    return db

# 导出数据命令
# arangoexport --type jsonl --collection leaguer --collection leaguer_edge --collection movie --collection person --collection personate_edge --collection relation_edge --server.database mtime --server.password 123456 --output-directory "dump"
# 导入数据命令
# ls |awk -F . '{print $1}'|xargs -i arangoimp --threads 4 --file "{}.jsonl" --type jsonl --collection "{}" --server.database mtime --server.password 123456