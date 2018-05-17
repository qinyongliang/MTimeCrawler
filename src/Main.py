#!/bin/python3
#-*- coding: UTF-8 -*-
import logging
from json import *
import time
from utils.request import *
from utils.db import *
from functools import reduce
import traceback
import threading
import sys

logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

movieInfoUrl = 'http://m.mtime.cn/Service/callback.mi/movie/Detail.api?movieId={}'
leaguerUrl = 'http://m.mtime.cn/Service/callback.mi/Movie/MovieCreditsWithTypes.api?movieId={}'
personInfoUrl = 'http://m.mtime.cn/Service/callback.mi/Person/Detail.api?personId={}'

participateListUrl = "http://m.mtime.cn/Service/callback.mi/Person/Movie.api?personId={}&pageIndex={}&orderId=0"

nameKeys = ['name','titleCn','nameCn','titleEn','nameEn','TitleCn','NameCn','TitleEn','NameEn']
def getName(data):
    for key in nameKeys:
        if(key in data.keys() and '' != data[key]):
            return data[key]


personateKeys = ['personate', 'personateCn', 'personateEn',
                 'Personate', 'PersonateCn', 'PersonateEn', ]
def getpersonate(data):
    for key in personateKeys:
        if(key in data.keys() and '' != data[key]):
            return data[key]

def crawlerMovie(id=253823,old = {}):
    movieInfo = get(movieInfoUrl.format(str(id))).json()
    movieInfo['id']=id
    
    movieInfo['name'] = getName(movieInfo)
    movieInfo['raw'] = False
    logger.info("\t成功获取到{}的信息,保存中".format(movieInfo['name']))
    
    #保存电影信息
    save('movie',movieInfo,id)
    
    if(have('leaguer',id)):
        return
    logger.info("\t开始获取{}的成员信息".format(movieInfo['name']))
    #获取成员信息
    leaguers = get(leaguerUrl.format(str(id))).json()
    leaguers = list(map(lambda types: list(map(lambda person: {**person,**{
            'typeName': types['typeName'],
            'typeNameEn': types['typeNameEn']
        }},
        types['persons'])), leaguers['types']))
    leaguers = list(reduce(lambda x, y: x + y, leaguers))
    logger.info("\t获取到{}的{}条成员信息，保存中".format(movieInfo['name'], len(leaguers)))
    save("leaguer",{
        "movieId":id,
        'leaguers': leaguers
    }, id)
    # 保存到代添加列表
    logger.info("\t将获取到{}的{}条成员信息记录到待爬取列表".format(
        movieInfo['name'], len(leaguers)))
    for leaguer in leaguers:
        try:
            if(not have('person',leaguer['id'])):
                save("person", {**leaguer,**{
                    "raw":True,
                    "name": getName(leaguer)
                }}, leaguer['id'])
        except :
            traceback.print_exc()

    logger.info("\t将获取到{}的{}条成员信息添加在关系链中".format(
        movieInfo['name'], len(leaguers)))
    # 保存到关系
    for person in leaguers:
        try:
            getdb()['leaguer_edge'].createEdge({
                '_from': 'movie/' + str(id),
                '_to': 'person/' + str(person['id']),
                'type': person['typeName']
            }).save()
        except:
            traceback.print_exc()

def crawlerPerson(id,old={}):
    #获取成员详情
    personInfo = get(personInfoUrl.format(str(id))).json()
    
    personInfo['name'] = getName(personInfo)
    personInfo['raw'] = False
    save('person', personInfo,id)
    logger.info("\t成功获取到{}的信息".format(personInfo['name']))
    if('relationPersons' in personInfo.keys()):
        #把关系保存下来
        logger.info("\t{}存在{}条人物关系,记录中".format(
            personInfo['name'], len(personInfo['relationPersons'])))
        for relationPerson in personInfo['relationPersons']:
            if(not have('person', relationPerson['rPersonId'])):
                #保存待处理
                data = {
                    'NameCn': relationPerson['rNameCn'],
                    'NameEn': relationPerson['rNameEn'],
                    "raw": True,
                }
                data['name'] = getName(data)
                save("person", data, relationPerson['rPersonId'])
            #保存边
            save('relation_edge',{
                '_from': 'person/' + str(id),
                '_to': 'person/' + str(relationPerson['rPersonId']),
                'type': relationPerson['relation']
                }, '{}_{}'.format(str(id), str(relationPerson['rPersonId'])),'edge')

    logger.info("\t{}参与了{}部作品，记录中".format(
        personInfo['name'], personInfo['movieCount']))
    #获取参演电影列表
    for page in range(1,int(personInfo['movieCount']/20)+2):
        movieList = get(participateListUrl.format(id,page)).json()
        for rawMovie in movieList:
            #保存到待爬取列表
            try:
                if(not have('movie',rawMovie['id'])):
                    save("movie", {**rawMovie, **{
                        "raw": True,
                        "name":getName(rawMovie)
                    }}, rawMovie['id'])
                personate = None
                if('personates' in rawMovie.keys()):
                    personate = '|'.join(
                        list(map(lambda v: v['name'], rawMovie['personates'])))
                else:
                    personate =  getpersonate(rawMovie)
                #保存扮演信息
                if(personate!=None and personate!=''):
                    save('personate_edge', {
                        '_from': 'person/' + str(id),
                        '_to': 'movie/' + str(rawMovie['id']),
                        'type': personate
                    }, '{}_{}'.format(str(id), str(rawMovie['id'])), 'edge')
            except:
                traceback.print_exc()
def personThread():
    while(True):
        nextPerson = next('person')
        if(nextPerson!=None):
            try:
                if(not have('person', nextPerson['id'])):
                    nextPerson['raw'] = False
                    nextPerson.save()
                    logger.info(u"开始获取成员{}的信息".format(nextPerson['name']))
                    crawlerPerson(nextPerson['id'],nextPerson)
            except:
                traceback.print_exc()
                logger.error('获取成员{}信息失败'.format(nextPerson['name']))
        else:
            logger.warning(u'没有找到待处理的成员，等待中')
            time.sleep(10)
def movieThread():
    while(True):
        nextMovie = next('movie')
        if(nextMovie != None):
            try:
                if(not have('movie', nextMovie['id'])):
                    nextMovie['raw'] = False
                    nextMovie.save()
                    logger.info(u"开始获取电影{}的信息".format(nextMovie['name']))
                    crawlerMovie(nextMovie['id'],nextMovie)
            except Exception:
                traceback.print_exc()
                logger.error('获取电影{}信息失败'.format(nextMovie['name']))
        else:
            logger.warning(u'没有找到待处理的电影，等待中')
            time.sleep(10)


if __name__ == '__main__':
    #先爬取一个电影再说
    if(getConnect('movie').count()==0):
        crawlerMovie(253823)
    if(len(sys.argv)>1):
        if(sys.argv[1]=='movie'):
            tread1 = threading.Thread(target=movieThread)
            tread1.start()
            tread1.join()
        if(sys.argv[1]=='person'):
            tread2=threading.Thread(target=personThread)
            tread2.start()
            tread2.join()
    else:
        tread1 = threading.Thread(target=movieThread)
        tread1.start()
        tread1.join()

        tread2=threading.Thread(target=personThread)
        tread2.start()
        tread2.join()
