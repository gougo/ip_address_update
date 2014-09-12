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


    def __init__(self, input, output):
        self.input_file=input
        self.output=open(output, 'w')


    def run(self):
        with open(self.input_file,"r") as inf:
            bf_ip_end = 0
            bf_ip_head = 0
            bf_address = ""
            for line in inf:
                iphead, ipend, address = line.strip().split('`',2)
                iphead = int(iphead)
                ipend = int(ipend)
                if address == bf_address:
                    if iphead - bf_ip_end <= self.MERGE_STANDARD:
                        bf_ip_end = ipend
                    else:
                        self.output.write("%s`%s`%s\n"%(bf_ip_head, bf_ip_end, bf_address))
                        bf_ip_head = iphead
                        bf_ip_end = ipend
                        bf_address = address
                else:
                    if bf_address:
                        self.output.write("%s`%s`%s\n"%(bf_ip_head, bf_ip_end, bf_address))
                    bf_ip_head = iphead
                    bf_ip_end = ipend
                    bf_address = address                    

        if bf_address:
            self.output.write("%s`%s`%s\n"%(bf_ip_head, bf_ip_end, bf_address))

if __name__=="__main__":
    '''
    1 input
    2 output
    '''
    ip_map=GenIPAddressMap(sys.argv[1], sys.argv[2])
    ip_map.run()