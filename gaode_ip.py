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
import Queue
import os
logging.basicConfig(filename = os.path.join(os.getcwd(), 'run_log'),
                    level = logging.DEBUG,
                    format = '%(asctime)s - %(levelname)s: %(message)s')

URL='http://restapi.amap.com/v3/geocode/regeo?location=%s,%s&key=5973fb6764b8093257dcfd6ff43f6746&s=rsv3&extensions=base'

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
    def __init__(self, qurl, mutex, result_log):
        threading.Thread.__init__(self)
        self.qurl=qurl
        # self.error_log = error_log
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
                    # adcode = s['regeocode']['addressComponent']['adcode']
                    if prov == []:
                        # self.error_log.write("nofound:%s\n"%ddc[1])
                        logging.error("nofound:%s\n"%ddc[1])
                        break
                    if city == []:
                        city = ''
                    self.result_file.write("%s`%s`%s\n"%(ddc[0], prov.encode('utf8'), city.encode('utf8')))
                except Exception,e:
                    logging.warn(e.args)
                    # self.error_log.write("%s`%s\n"%ddc)
                    logging.error("%s`%s\n"%ddc)
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

    def __init__(self, input_file, queue):
        threading.Thread.__init__(self)
        self.input_file = input_file
        self.qurl=queue

    def run(self):
        with open(self.input_file,"r") as ip_file:
            for line in ip_file:
                line=line.strip()
                rec = line.split('`')
                if len(rec) !=3:
                    logging.error("fielderr:%s\n"%line)
                    # self.error_line.write("fielderr:%s\n"%line)
                    continue
                if float(rec[-2])>0  and float(rec[-1])>0:
                    lat = '%.4f'%(float(float(rec[-1])/float(360000)))
                    lon = '%.4f'%(float(float(rec[-2])/float(360000)))
                else:
                    continue

                _url = URL % (lat, lon)
                self.qurl.put((rec[0], _url), block=True)

        global READ_FINISH
        READ_FINISH=True

class Worker(object):
    qurl=Queue.Queue()
    mutex = threading.Lock()
    threadCount=9    #开启线程数，默认6个线程

    def __init__(self, input, output):
        self.error_line=open("error_line", "w")
        self.result_file=open(output, "w")
        self.ths = []
        self.input_file = input

    def __del__(self):
        if self.result_file:
            self.result_file.close()
        if self.error_line:
            self.error_line.close()


    def init_ths(self):
        for t in range(self.threadCount):
            thread=ThreadWork(self.qurl, self.mutex, self.result_file)
            # thread.setDaemon(True)
            thread.start()
            self.ths.append(thread)

    # def readIP(self):
    #     with open(self.input_file,"r") as ip_file:
    #         for line in ip_file:
    #             line=line.strip()
    #             rec = line.split('`')
    #             if len(rec) !=3:
    #                 self.error_line.write("fielderr:%s\n"%line)
    #                 continue
    #             if float(rec[-2])>0  and float(rec[-1])>0:
    #                 lat = '%.4f'%(float(float(rec[-1])/float(360000)))
    #                 lon = '%.4f'%(float(float(rec[-2])/float(360000)))
    #
    #             _url = URL % (lat, lon)
    #             self.qurl.put((rec[0], _url), block=True)
    #     global READ_FINISH
    #     READ_FINISH=True

    def wait(self):
        for t in self.ths:
            t.join(5)

    def run(self):
        # self.readIP()
        producer= Producer(self.input_file, self.qurl)
        producer.start()
        self.init_ths()
        producer.join()
        self.wait()
        # self.qurl.join()

if __name__ == "__main__":
    '''
    1 input
    2 output
    '''
    worker=Worker(sys.argv[1], sys.argv[2])
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

