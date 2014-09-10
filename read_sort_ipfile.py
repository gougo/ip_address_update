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
logging.basicConfig(filename = os.path.join(os.getcwd(), 'sort_ipfile.log'),
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
            bf_ip = ""
            bf_address = []
            discard_ip = ""
            head_ip = ""
            for line in inf:
                ip, address = line.strip().split('`',1)
                if ip==discard_ip:
                    continue
                if ip==bf_ip:# ip same
                    if address in bf_address:
                        continue
                    else: # 地址不同放弃
                        bf_address.append(address)
                        discard_ip = ip
                        continue
                else:
                    if len(bf_address) == 1:
                        if address == bf_address[0]:
                            diff = int(ip)-int(bf_ip)
                            if diff>self.MERGE_STANDARD:
                                logging.info("diff:%s", diff)
                                self.output.write("%s`%s`%s\n" % (head_ip, bf_ip, bf_address[0]))
                                head_ip=ip
                            bf_ip=ip
                        else:
                            # TODO 输出前面的ip信息
                            self.output.write("%s`%s`%s\n" % (head_ip, bf_ip, bf_address[0]))

                            head_ip=ip
                            bf_ip=ip
                            bf_address=[address]
                    else: # 前一个ip有多个地址或初始。重新开始记录
                        logging.debug("%s has multi address . discard.", bf_ip)
                        head_ip=ip
                        bf_ip=ip
                        bf_address=[address]

            # TODO 最终输出
            if len(bf_address) == 1:
                self.output.write("%s`%s`%s\n" % (head_ip, bf_ip, bf_address[0]))
            else:
                logging.debug("%s has multi address . discard.", bf_ip)



if __name__=="__main__":
    '''
    1 input
    2 output
    '''
    ip_map=GenIPAddressMap(sys.argv[1], sys.argv[2])
    ip_map.run()