#coding=gbk
import thread, time, random
count = 0
def threadTest():
    global count
    for i in xrange(10000):
        count += 1
for i in range(10):
    thread.start_new_thread(threadTest, ())	#�����start_new_thread�������Ǻ��˽⣬��Ҫ�ż������Ͼͻὲ��
time.sleep(3)
print count	#count�Ƕ����أ���10000 * 10 ��