# -*- encoding: utf-8 -*-

import sys
import os
import iptools
from wingsoft_map import *
from zcode_dict import *

isp_err = open("err/check-google_err", "w+");

while True:
    #print prov_map
    lines = sys.stdin.readlines(300000)
    if not lines: break
    for line in lines:
        line = line.strip()
        if '' == line: continue
        rec = line.split('`')       
        if len(rec)<4:
            continue
        (intip_beg,intip_end,prov,city) = rec
        ip_begstr = iptools.long2ip(long(intip_beg))
        ip_endstr = iptools.long2ip(long(intip_end))
        if prov != "" and city == "":
            if prov in prov_map:
                prov = prov_map[prov]
            else:
                print >>isp_err, "prov`%s" %(prov)
        elif prov != "" and city != "":
	    if prov in ["北京市","天津市","上海市","重庆市"]:
		    city = ''  
            key = prov + "," + city
            if key in prov_city_map:
                (prov, city) = prov_city_map[key].split(",")
            else:
                print >>isp_err, "prov_city`%s`%s" %(prov,city)
        elif prov == "" and city != "":
            print >>isp_err, "city`%s`%s" %(prov,city)

        if prov in ["北京","天津","上海","重庆"]:
            city = prov
        zcode = ''
        if city in city_zcode_dict:
	    zcode = city_zcode_dict[city]
        print "%s`%s`中国`CN`%s`%s`%s``%s`%s"%(ip_begstr,ip_endstr,prov,city,zcode,intip_beg,intip_end)

