# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2014-9-2

@author: tianfei
'''
import urllib2
import sys
import json
from xml.dom import minidom
import time
import logging
import traceback
import threading
import Queue

IPFILE="get250.china.seq.log_9"
URL='http://restapi.amap.com/v3/geocode/regeo?location=%s,%s&key=5973fb6764b8093257dcfd6ff43f6746&s=rsv3&radius=1000&extensions=none'

def deco(func):
    def _deco(*args, **kwargs):
        retry_sec = 30
        ret = None
        count = 0
        while True:
            try:
                ret = func(*args, **kwargs)     
                return ret          
            except Exception, e:
                if count > 3 :
                    raise 
                count += 1       
                time.sleep(retry_sec + count*10)         
        return False
    return _deco
def get_attrvalue(node, attrname):
    return node.getAttribute(attrname) if node else ''

def get_nodevalue(node, index = 0):
    return node.childNodes[index].nodeValue if node else ''

def get_xmlnode(node,name):
    return node.getElementsByTagName(name) if node else []

def xml_to_string(filename='user.xml'):
    doc = minidom.parse(filename)
    return doc.toxml('UTF-8')

def get_xml_data(filename='user.xml'):
    print filename
    doc = minidom.parse(filename) 
    root = doc.documentElement
    status = get_xmlnode(root,'status')
    status_string = get_nodevalue(status[0]).encode('utf-8','ignore')
    if status_string.lower() != 'ok':
        return filename,''
    result_nodes = get_xmlnode(root,'result')
    address_nodes = get_xmlnode(result_nodes[0],'address_component')
    prov = ''
    city = ''
    coun = ''
    for node in address_nodes: 
        node_type_1 = get_xmlnode(node,'type')
        node_type_2 = get_xmlnode(node,'type')
        node_short_name = get_xmlnode(node,'short_name')
        ip_nodetype1_name =get_nodevalue(node_type_1[0]).encode('utf-8','ignore')
        ip_nodetype2_name = get_nodevalue(node_type_2[0]).encode('utf-8','ignore') 
        ip_short_name = get_nodevalue(node_short_name[0]).encode('utf-8','ignore') 
        if ip_nodetype1_name in ['administrative_area_level_1','locality','country'] \
            or ip_nodetype2_name in ['administrative_area_level_1','locality','country']:            
            if ip_nodetype1_name == 'country' or ip_nodetype2_name == 'country':
                coun = ip_short_name
            elif ip_nodetype1_name == 'administrative_area_level_1' or ip_nodetype2_name == 'administrative_area_level_1':
                prov = ip_short_name 
            elif ip_nodetype1_name == 'locality' or ip_nodetype2_name == 'locality':
                city = ip_short_name
                if city.find("市")>=0 or city.find("区")>=0:
                    city = city
                else:
                    city = city + "市"
    print coun,prov,city
    return coun,prov,city

@deco
def get_info(url):
    # print url
    try:
        response = urllib2.urlopen(url)
        if response is None:
            return None
        else:
            return response.read()
    except urllib2.HTTPError, e:
        logging.warn("saving transmited result to scheduler failed, remote server response: %s, %s",e.code, e.read())
        raise
    except urllib2.URLError, e:
        logging.warn("saving transmited result to scheduler failed, and remote server has no response: %s",
                    e.args)
        raise
    except Exception, e:
        logging.warn('%s\n%s',e.args, traceback.format_exc())
        return False

class ThreadWork(threading.Thread):
    def __init__(self, qurl, mutex, error_log, result_log):
        threading.Thread.__init__(self)
        self.qurl=qurl
        self.error_log = error_log
        self.result_file = result_log
        self.lock=mutex
        # self.cond=threading.Condition()

    def run(self):
        global READ_FINISH
        while True:
            if self.qurl.qsize()==0:
                if READ_FINISH:
                    break
                else:
                    time.sleep(1)
                    continue
            self.lock.acquire()
            ddc=self.qurl.get(block=True, timeout=5)
            self.lock.release()
            if ddc is None:
                break
            while True:
                gdjson=get_info(ddc[1])
                # self.lock.acquire()
                try:
                    s=json.loads(gdjson)
                    prov =  s['regeocode']['addressComponent']['province']
                    city = s['regeocode']['addressComponent']['city']
                    if prov == []:
                        self.error_log.write("nofound:%s\n"%ddc[1])
                        break
                    if city == []:
                        city = ''
                    self.result_file.write("%s`%s`%s\n"%(ddc[0], prov.encode('utf8'), city.encode('utf8')))
                except Exception,e:
                    logging.error(e.args)
                    self.error_log.write("%s`%s\n"%ddc)
                break
            # self.lock.release()

# qurl=Queue.Queue(0)
# threadCount=6    #开启线程数，默认6个线程

# ths=[]
# for t in range(threadCount):
# 	thread=th(qurl)
# 	thread.start()
# 	ths.append(thread)
READ_FINISH=False
class Producer(threading.Thread):

    def __init__(self, t_name, queue):
        threading.Thread.__init__(self,name=t_name)
        self.qurl=queue

    def run(self):
        with open(IPFILE,"r") as ip_file:
            for line in ip_file:
                line=line.strip()
                rec = line.split('`')
                if len(rec) !=6:
                    self.error_line.write("fielderr:%s\n"%line)
                    continue
                if float(rec[-2])>0  and float(rec[-1])>0:
                    lat = '%.4f'%(float(float(rec[-1])/float(360000)))
                    lon = '%.4f'%(float(float(rec[-2])/float(360000)))

                _url = URL % (lat, lon)
                self.qurl.put((rec[0], _url), block=True)

        global READ_FINISH
        READ_FINISH=True

class Worker(object):
    qurl=Queue.Queue()
    mutex = threading.Lock()
    threadCount=6    #开启线程数，默认6个线程

    def __init__(self):
        self.error_line=open("error_line", "w")
        self.result_file=open("tf_ip_add", "w")
        self.ths = []

    def init_ths(self):
        for t in range(self.threadCount):
            thread=ThreadWork(self.qurl, self.mutex, self.error_line, self.result_file)
            # thread.setDaemon(True)
            thread.start()
            self.ths.append(thread)

    def readIP(self):
        with open(IPFILE,"r") as ip_file:
            for line in ip_file:
                line=line.strip()
                rec = line.split('`')
                if len(rec) !=6:
                    self.error_line.write("fielderr:%s\n"%line)
                    continue
                if float(rec[-2])>0  and float(rec[-1])>0:
                    lat = '%.4f'%(float(float(rec[-1])/float(360000)))
                    lon = '%.4f'%(float(float(rec[-2])/float(360000)))

                _url = URL % (lat, lon)
                self.qurl.put((rec[0], _url), block=True)
        global READ_FINISH
        READ_FINISH=True

    def wait(self):
        for t in self.ths:
            t.join(5)

    def run(self):
        # self.readIP()
        producer= Producer("Pro", self.qurl)
        producer.start()
        self.init_ths()
        producer.join()
        self.wait()
        # self.qurl.join()

class IP2Addr(object):
    url='http://restapi.amap.com/v3/geocode/regeo?location=%s,%s&key=5973fb6764b8093257dcfd6ff43f6746&s=rsv3&radius=1000&extensions=none'
    # ipfile="get250.china.seq.log"

    def __init__(self):
        self.error_line=open("error_line", "w")
        self.result_file=open("tf_ip_add", "w")

    def get_add(self, lat, lon):
        _url = self.url % (lat, lon)
        result=get_info(_url)
        if result:
            try:
                s = json.loads(result)
                prov = s['regeocode']['addressComponent']['province']
                city = s['regeocode']['addressComponent']['city']
                if prov == []:
                    # prov = ''
                    self.error_line.write("nofound:%s\n"%_url)
                    return False
                if city == []:
                    city = ''

                return (prov.encode('utf8'), city.encode('utf8'))
            except Exception, e:
                logging.error(e.args)
        return False

    def readIP(self):
        with open(IPFILE,"r") as ip_file:
            for line in ip_file:
                line=line.strip()
                rec = line.split('`')
                if len(rec) !=6:
                    self.error_line.write("fielderr:%s\n"%line)
                    continue

                if float(rec[-2])>0  and float(rec[-1])>0:
                    lat = '%.4f'%(float(float(rec[-1])/float(360000)))
                    lon = '%.4f'%(float(float(rec[-2])/float(360000)))
                    result=self.get_add(lat, lon)
                    if result:
                        self.result_file.write("%s`%s`%s\n" % (rec[0], result[0], result[1]))
                    else:
                        self.error_line.write("adderr:%s\n"%line)
                else:
                    self.error_line.write("jwerr:%s\n"%line)

    def __del__(self):
        if self.result_file:
            self.result_file.close()
        if self.error_line:
            self.error_line.close()


    def run(self):
        self.readIP()




if __name__ == "__main__":
    worker=Worker()
    worker.run()

    # qurl=Queue.Queue(100)
    # error_line=open("error_line", "w")
    # result_file=open("tf_ip_add", "w")
    # ths = []
    # mutex = threading.Lock()
    # producer= Producer("Pro", qurl)
    # for t in range(6):
    #     thread = ThreadWork(qurl, mutex, error_line, result_file)
    #     ths.append(thread)
    # for t in ths:
    #     t.setDaemon(True)
    #     t.start()
    # producer.start()
    # producer.join()
    # for t in ths:
    #     t.join()

