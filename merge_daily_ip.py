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
    def __init__(self, all_file, daily_file):
        self.today_all = all_file + ".new"
        self.output = open(self.today_all, 'w')
        self.input_today = open(daily_file, 'r')
        self.input_all = open(all_file, 'r')

    def read_new(self):
        try:
            line_t=self.input_today.next()
        except Exception:
            logging.debug("finish read new file")
            self.input_today.close()
            self.input_today = None
            return False
        return line_t


    def merge(self):
        before_new_head = before_new_end= before_all_head=before_all_end=0
        before_address = ""
        new_file_end = False
        for line_all in self.input_all:
            if not self.input_today and not new_file_end: # 新文件已经读完
                self.output.write(line_all)
                continue
            all_head, all_end, all_address = line_all.strip().split('`',2)
            all_head=long(all_head)
            all_end=long(all_end)

            if before_address and before_address != all_address:
                if all_end < before_new_head:
                    self.output.write("%s`%s`%s\n" %(all_head, all_end, all_address) )
                    continue
                else:
                    if all_head > before_new_end:
                        self.output.write("%s`%s`%s\n"%(before_new_head, before_new_end, before_address))
                    else:
                        if all_end <= before_new_end:
                            continue
                        else:
                            self.output.write("%s`%s`%s\n"%(before_new_head, before_new_end, before_address))
                            all_head = before_new_end+1   # 新的覆盖旧的
            else:
                if (all_end+1)< before_new_head:
                    self.output.write("%s`%s`%s\n" %(all_head, all_end, all_address) )
                    continue
                elif (all_end+1) == before_new_head:
                    before_new_head = all_head
                    continue
                else:
                    if all_end<= before_new_end:
                        continue
                    elif before_new_head>0:
                        if (all_head-1)> before_new_end:
                            self.output.write("%s`%s`%s\n"%(before_new_head, before_new_end, before_address))
                        else:
                            all_head = before_new_head
            new_file_end = False
            if self.input_today:
                line_t =  self.read_new()
                while line_t:
                    new_head, new_end, new_address = line_t.strip().split('`',2)
                    new_head = long(new_head)
                    new_end=long(new_end)

                    if all_address == new_address:
                        if new_head >= all_head:
                            if new_end <= all_end:
                                line_t =  self.read_new()
                                continue
                            elif (new_head-1) <= all_end:
                                before_new_head = all_head
                                before_new_end = new_end
                                before_address = new_address
                                new_file_end=True
                                break
                            else:
                                self.output.write("%s`%s`%s\n"%(all_head, all_end, all_address))
                                before_new_end=new_end
                                before_new_head = new_head
                                before_address = new_address
                                new_file_end=True
                                break
                        else:
                            if (new_end+1) < all_head:
                                self.output.write("%s`%s`%s\n"%(new_head, new_end, new_address))
                                line_t =  self.read_new()
                                continue
                            elif (new_end+1) == all_head:
                                all_head=new_head
                                line_t = self.read_new()
                                continue
                            else:
                                all_head=new_head
                                if new_end <= all_end:
                                    line_t =  self.read_new()
                                    continue
                                else:
                                    before_new_end=new_end
                                    before_new_head = new_head
                                    before_address = new_address
                                    new_file_end=True
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
                                if all_end < new_head:
                                    self.output.write("%s`%s`%s\n"%(all_head, all_end, all_address))
                                else:
                                    self.output.write("%s`%s`%s\n"%(all_head, new_head-1, all_address))
                                before_new_end = new_end
                                before_new_head = new_head
                                before_address = new_address
                                new_file_end=True
                                break
                        else:
                            if new_end<all_end:
                                if new_end<all_head:
                                    self.output.write("%s`%s`%s\n" % (new_head, new_end, new_address))
                                    line_t =  self.read_new()
                                    continue
                                else:
                                    self.output.write("%s`%s`%s\n" % (new_head, new_end, new_address))
                                    all_head=new_end+1
                                    line_t =  self.read_new()
                                    continue
                            else:
                                before_new_end = new_end
                                before_new_head = new_head
                                before_address = new_address
                                new_file_end=True
                                break
                else:
                    self.output.write("%s`%s`%s\n" %(all_head, all_end, all_address))
            else:
                self.output.write("%s`%s`%s\n" % (all_head, all_end, all_address))

        if new_file_end:
            self.output.write("%s`%s`%s\n" % (before_new_head, before_new_end, before_address))
            line_t =  self.read_new()
            while line_t:
                self.output.write(line_t)
                line_t =  self.read_new()


if __name__ == '__main__':
    '''
    1 all
    2 daily
    '''
    mif = MergeIPFile(sys.argv[1], sys.argv[2])
    mif.merge()