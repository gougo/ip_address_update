# -*- encoding:utf8 -*-
import re
import MySQLdb
import sys
import threading
from datetime import datetime
from Queue import Queue
from sphinxapi import *
#部分法语介词列表
spacewords = ['\xc3\xa0', 'de', 'de la', 'du', 'des', 'le', 'la', 'les', 'ce', 'ci', 'car', 
        'avec', 'bon', 'bonne', 'bien', 'dans', 'en', 'un', 'que', 'qui','pour', 'chacun',
        'entre', 'il faut', 'jamais', 'pas', 'pas de', 'quand','ou','ch','et','sa','par','a','A']
#索引列表
indexs = ['wakazaka_fr_ebay','wakazaka_fr_kelkoo','wakazaka_fr_article','wakazaka_fr_priceminister']
host = ''
name = ''
password =''
db = 'wakazaka_fr_innodb'
#Sphinx端口
port = 9312
#匹配模式
mode = SPH_MATCH_PHRASE
#索引
INDEXS= "wakazaka_fr;wakazaka_fr_ebay;wakazaka_fr_kelkoo;wakazaka_fr_article;wakazaka_fr_priceminister"


class Mythread(threading.Thread):
    def __init__(self,queue):
        threading.Thread.__init__(self)
        self.queue = queue
    def run(self):
        global mutex,sc
        while True:
        #获取线程的名字
        #name = threading.currentThread().getName()
            item = self.queue.get()
            if self.queue.empty():
              break
            else:
              try:
                process(item[0],item[1],sc,mutex)
              except:
                self.queue.put(item)
              self.queue.task_done()
        print 'Program is over'
        

#构建KEYWORD，不能含有介词和非数字和字母，KEYWORD的长度不能大于3
def complex_words(title):
    li = title.split(' ')
    words = []
    i = 0
    #print li
    length = len(li)
    count = length
    while count > 1:
        j = i + 1
        #print 'j',j
        if j + 1 < length:
            if re.search('\W',li[i]) or li[i] in spacewords:
                i = j
                count -= 1
                continue
            elif re.search('\W',li[j]) or li[j] in spacewords:
                i = j + 1
                count -= 2
                continue
            elif li[i] not in spacewords and li[j] not in spacewords:
                tmp = li[i] + ' ' + li[j]
                words.append(tmp)
                #print '1',words
            if re.search('\W',li[j+1]) or li[j+1] in spacewords:
                i = j + 2
                count -= 3
                continue
            elif li[j+1] not in spacewords:
                words.append(tmp + ' ' + li[j+1])
            i += 1
            count -= 1
            #print '2',words
        else:
            if re.search('\W',li[i]) or li[i] in spacewords:
                count -= 1
                break

            elif re.search('\W',li[j]) or li[j] in spacewords:
                count -= 1
                break
            elif li[i] not in spacewords and li[j] not in spacewords:
                words.append(li[i] + ' ' + li[j])
            count -= 1
            i += 1
            #print '3',words
    #print words
    return words
#获取文本
def get_item(i,j):
    conn = MySQLdb.connect(host,name,password,db)
    if conn:
        
        #print "Connect to the database"
        conn.set_character_set("utf8")
        cursor = conn.cursor()
        strsql = "SELECT title,category_id FROM articles ORDER BY articles.id ASC LIMIT %s,%s"%(i,j)
        #print strsql
        cursor.execute(strsql)
        data = cursor.fetchall()
        conn.close()
        return data
#插入数据
def write_item(keyword,cid,length,hits):
    conn = MySQLdb.connect(host,name,password,db)
    if conn:
        #print "Connect to the database"
        conn.set_character_set("utf8")
        cursor = conn.cursor()
        strsql = "INSERT INTO test_new (keyword,category_id,length,hits) VALUES ('%s','%s','%s','%s')\
            "%(keyword,cid,length,hits)
        print strsql
        cursor.execute(strsql)
        conn.close()
#判断数据是否重复
def check(keyword,cid):
    conn = MySQLdb.connect(host,name,password,db)
    if conn:
        #print "Connect to the database"
        conn.set_character_set("utf8")
        cursor = conn.cursor()
        strsql = "SELECT id FROM test_new WHERE keyword = '%s' AND category_id = '%s'"%(keyword,cid)
        #print strsql
        cursor.execute(strsql)
        if cursor.rowcount == 0:
            conn.close()
            return None
        else:
            conn.close()
            return cursor.fetchone()
        
#查询KEYWORD，返回其个数
def sphinx_result(keyword,sc):
    sc.SetServer(host,port)
    sc.SetMatchMode(mode)
    li = keyword.split(' ')
    length = len(li)
    sc.SetGroupBy('category_id',SPH_GROUPBY_ATTR,"@count desc")
    
    result = sc.Query(keyword,INDEXS)
    maxcount = 0
    mylist = []
    
    if result.has_key('matches'):
        for num,match in enumerate(result["matches"]):
            cid = match["attrs"]["category_id"]
            count = match["attrs"]["@count"]
            maxcount += count
            
            if num > 2:
                break
            mylist.append([keyword,cid,length,count])
    return mylist
#Sphinx的BuildKeywords方法会将输入的文本拆分，并得出在数据库中的个数
def single_words(title,sc,index):
   global mutex
   #这一段请查看Sphinx API，有详细说明
   sc.SetServer(host,port)
   sc.SetMatchMode(mode)
   mutex.acquire()
   results = sc.BuildKeywords(title,index,True)
   mutex.release()
   return results

def get_count(map,sc,title):
    for index in indexs:
        results = single_words(title,sc,index)
        for result in results:
            name = result['tokenized']
            hits = result['hits']
            for key in map.keys():
                if name == key:
                    map[name] = map[name] + hits
    return map

def process(title,cid,sc,mutex):
    map = {}
    dict = {}
    results1 = single_words(title,sc,'wakazaka_fr')
    for result in results1:
        name = result['tokenized']
        if re.search('\W+',name):
            continue
        elif name not in spacewords and len(name) >= 3:
            try:
                int(name)
            except:
                if check(name,cid):
                    continue
                else:
                    hits = result['hits']
                    map[name] = hits
                    dict = get_count(map,sc,title)
        if dict:
            for key in dict.keys():
                try:
                    mutex.acquire()
                    write_item(key.lower(),cid,1,dict[key])
                    mutex.release()
                except:
                    mutex.release()
                    print 'Exist'
                    continue
    
    results2 = complex_words(title)
    for result in results2:
        result = result.strip()
        if len(result) >= 3 and result not in spacewords: 
            if check(result,cid):
                continue
            else:
                li = sphinx_result(result,sc)
                for l in li:
                    try:
                        int(l[0])
                    except:
                        temp = l[0].split(' ')[0]
                        if len(temp) > 1:
                            try:
                                int(l[0])
                            except:
                                try:
                                    mutex.acquire()
                                    write_item(l[0].lower(),l[1],l[2],l[3])
                                    mutex.release()
                                except:
                                    mutex.release()
                                    print 'Exist'
                                    continue
                                  
def main():
  start_time = datetime.now()
  global mutex,sc
  threads = []
  q = Queue()
  mutex = threading.Lock()
  #建立Sphinx对象
  sc = SphinxClient()
  for r in range(0,50):
    threads.append(Mythread(q))
    
  for t in threads:
    t.setDaemon(True)
    t.start()
  i = 0
  j = 1000
  #获取数据，每次取1000条，直至取完为止
  items = get_item(i,j)
  temp = 1
  while len(items) > 0:
    for item in items:
        q.put(item)
    i = temp + j
    items = get_item(i,j)
    temp = i
    
    for t in threads:
        t.join()
    q.join()
    end_time = datetime.now()
    print '###TIME:',end_time - start_time

if __name__ == "__main__":
    main()
  