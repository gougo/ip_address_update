# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Created on 2014-9-2

@author: tianfei
'''
import urllib2
import sys
import json
import time
import logging
import traceback
import threading
# import pycurl,StringIO
import Queue

URL='http://maps.google.com/maps/api/geocode/json?latlng=%s,%s&language=en&sensor=false'

def deco(func):
    def _deco(*args, **kwargs):
        retry_sec = 10
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
                time.sleep(retry_sec + count*5)
        return False
    return _deco

@deco
def get_info(url):
    # print url
    try:
        req=urllib2.Request(url=url,
                        headers={'User-Agent':'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)'})
        response = urllib2.urlopen(req)

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

class ThreadWork(object):
    def __init__(self, error_log, result_log):
        self.error_log = error_log
        self.result_file = result_log

    def run(self, ddc):
        retry = 3
        while retry:
            gdjson=get_info(ddc[1])
            try:
                json_str=''
                for line in gdjson:
                    json_str += line.strip(" \n")
                s=json.loads(json_str)
                prov = ''
                city = ''
                country=''
                # adcode=0
                if s['status'] == 'OK':
                    if s['results']:
                        for address in s['results'][0]['address_components']:
                            _types = address.get('types', [])
                            if 'postal_code' in _types: # 邮编
                                pass
                                # adcode = address['short_name']
                            elif 'political' in _types:
                                if 'country' in _types: # 国家
                                    country=address['long_name']
                                if 'administrative_area_level_1' in _types:
                                    prov = address['short_name']
                                if 'locality' in _types:
                                    city = address['short_name']
                elif s['status'] == "ZERO_RESULTS":
                    self.error_log.write("%s nofound:%s\n"%ddc)
                    return
                elif s['status'] == "OVER_QUERY_LIMIT":
                    time.sleep(10)
                    retry -= 1
                    print "OVER_QUERY_LIMIT"
                    continue
                else:
                    self.error_log.write("error status:%s .%s %s",s['status'], ddc[0], ddc[1])

                if not prov:
                    self.error_log.write("%s no prov:%s\n"%ddc)
                    return
                self.result_file.write("%s`%s`%s`%s\n"%(ddc[0], country, unicode(prov).encode('utf-8'), unicode(city).encode('utf-8')))
            except Exception,e:
                logging.error(e.args)
                self.error_log.write("%s`%s\n"%ddc)
            return

class Worker(object):

    def __init__(self, input, output):
        self.error_line=open("error_line_google", "w")
        self.result_file=open(output, "w")
        self.ths = []
        self.input_file = input

    def __del__(self):
        if self.result_file:
            self.result_file.close()
        if self.error_line:
            self.error_line.close()

    def readIP(self):
        with open(self.input_file,"r") as ip_file:
            for line in ip_file:
                line=line.strip()
                rec = line.split('`')
                if len(rec) !=3:
                    self.error_line.write("fielderr:%s\n"%line)
                    continue
                if float(rec[-2])>0  and float(rec[-1])>0:
                    lat = '%.4f'%(float(float(rec[-1])/float(360000)))
                    lon = '%.4f'%(float(float(rec[-2])/float(360000)))

                _url = URL % (lon, lat)

                yield (rec[0], _url)


    def run(self):
        worker = ThreadWork(self.error_line, self.result_file)
        for ddc in self.readIP():
            worker.run(ddc)
            time.sleep(1)

if __name__ == "__main__":
    worker=Worker(sys.argv[1], sys.argv[2])
    worker.run()
    # a=u'''1694648123`Thailand`\u0e08.\u0e2b\u0e19\u0e2d\u0e07\u0e1a\u0e31\u0e27\u0e25\u0e33\u0e20\u0e39`HanNaNgam'''
    # a.encode('utf8')
    # print "%s" % a
    # url='http://maps.google.com/maps/api/geocode/json?latlng=39.5127,106.7164&language=en-US&sensor=false'
    # req=urllib2.Request(url=url,
    #                     headers={'User-Agent':'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)'})
    # response = urllib2.urlopen(req)
    #
    # print response.read()
    # aaa= response.read()
    # print json.loads(aaa)
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

