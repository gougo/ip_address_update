# !/usr/bin/env python
# -*- coding: utf-8 -*-
'''
将天的ip段合并到全量的ip段中
Created on 2014-9-2

@author: tianfei
'''

import logging
import sys
import os
logging.basicConfig(filename = os.path.join(os.getcwd(), 'merge_ipfile.log'),
                    level = logging.DEBUG,
                    format = '%(asctime)s - %(levelname)s: %(message)s')

class MergeIPFile(object):
    def __init__(self, daily_file, all_file):
        self.today_all = all_file + ".new"
        self.output = open(self.today_all, 'w')
        self.input_today = open(daily_file, 'r')
        self.input_all = open(all_file, 'r')

    def read_new(self):
        try:
            line_t=self.input_today.next()
            if len(line_t)>0:
                return line_t
        except Exception:
            logging.debug("finish read new file")
            self.input_today.close()
            self.input_today = None
        return False


    def merge(self):
        before_new_head = before_new_end= before_all_head=before_all_end=0
        before_address = ""
        for line_all in self.input_all:
            all_head, all_end, all_address = line_all.strip().split('`',2)
            all_head=int(all_head)
            all_end=int(all_end)
            if all_end <= before_new_end:
                continue
            else:
                if before_address and before_address != all_address:
                    self.output.write("%s`%s`%s\n"%(before_new_head, before_new_end, before_address))
                    all_head = before_new_end+1   # 新的覆盖旧的
                else:
                    all_head = before_new_head

            if self.input_today:
                line_t =  self.read_new()
                
                while line_t:
                    new_head, new_end, new_address = line_t.strip().split('`',2)
                    new_head = int(new_head)
                    new_end=int(new_end)

                    if all_address == new_address:
                        if new_head > all_head:
                            if new_end <= all_end:
                                line_t =  self.read_new()
                                continue
                            else:
                                before_new_end=new_end
                                before_new_head = new_head
                                before_address = new_address
                                # self.output.write("%s`%s`%s\n"%(all_head, new_end, all_address))
                                break
                        else:
                            all_head=new_head
                            if new_end <= all_end:
                                line_t =  self.read_new()
                                continue
                            else:
                                before_new_end=new_end
                                before_new_head = new_head
                                before_address = new_address
                                # self.output.write("%s`%s`%s\n"%(all_head, new_end, all_address))
                                break
                    else:
                        if new_head > all_head:
                            if new_end<all_end:
                                self.output.write("%s`%s`%s\n"%(all_head, new_head-1, all_address))
                                self.output.write("%s`%s`%s\n"%(new_head, new_end, new_address))
                                all_head=new_end+1
                                line_t =  self.read_new()
                                continue
                            else:
                                self.output.write("%s`%s`%s\n"%(all_head, new_head-1, all_address))
                                before_new_end=new_end
                                before_new_head = new_head
                                before_address = new_address
                                line_t =  self.read_new()
                                continue
                        else:
                            if new_end<all_end:
                                self.output.write("%s`%s`%s\n"%(new_head, new_end, new_address))
                                all_head = new_end+1
                                line_t=self.read_new()
                                continue
                            else:
                                before_new_end=new_end
                                before_new_head = new_head
                                before_address = new_address
                                break
                else:
                    self.output.write("%s`%s`%s\n"%(all_head, all_end, all_address))
            else:
                self.output.write("%s`%s`%s\n" %(all_head, all_end, all_address))

if __name__=='__main__':
    mif=MergeIPFile(sys.argv[1], sys.argv[2])
    mif.merge()
