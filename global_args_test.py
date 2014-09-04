#coding=gbk
import thread, time, random
count = 0
def threadTest():
    global count
    for i in xrange(10000):
        count += 1
for i in range(10):
    thread.start_new_thread(threadTest, ())	#如果对start_new_thread函数不是很了解，不要着急，马上就会讲解
time.sleep(3)
print count	#count是多少呢？是10000 * 10 吗？