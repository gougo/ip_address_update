# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
读取排序的ip文件.
排除一个ip多个地址的情况，并且将可以合并的ip段合并
Created on 2014-9-2

@author: tianfei
'''
import logging
import sys
import os
logging.basicConfig(filename = os.path.join(os.getcwd(), 'merge_ip.log'),
                    level = logging.DEBUG,
                    format = '%(asctime)s - %(levelname)s: %(message)s')


class GenIPAddressMap(object):
    '''
    生成ip地址映射
    '''
    # merge factors 间隔多少内可以合并
    MERGE_STANDARD = 1


    def __init__(self, input):
        self.input_file=input


    def run(self):
        diff_dict = {}
        diff_dict_2 = {}
        with open(self.input_file,"r") as inf:
            bf_ip_end = 0
            bf_ip_head = 0
            bf_address = ""
            for line in inf:
                iphead, ipend, address = line.strip().split('`',2)
                iphead = long(iphead)
                ipend = long(ipend)
                if iphead< bf_ip_end:
                    print "a"+line
                    return
                diff = iphead - bf_ip_end
                if address == bf_address:
                    if diff==1:
                        print "b"+line
                        return
                    diff_dict.setdefault(diff, 0)
                    diff_dict[diff] +=1
                else:
                    if bf_address:
                        diff_dict_2.setdefault(diff,0)
                        diff_dict_2[diff]+=1
                bf_ip_end = ipend
                bf_address = address

        for key, value in sorted(diff_dict.items(),  key=lambda d: d[0]):
            print "diff:%s, num:%s" % (key, value)

        print "========================================================"
        for key, value in sorted(diff_dict_2.items(), key=lambda d:d[0]):
            print "diff:%s, num:%s" % (key, value)

if __name__=="__main__":
    '''
    1 input
    2 output
    '''
    ip_map=GenIPAddressMap(sys.argv[1])
    ip_map.run()