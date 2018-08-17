
#import findspark
#findspark.init()
import math
from collections import OrderedDict
import pyspark
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import time

sc = SparkContext("local[2]", "Task2Trial4")  # new context
ssc = StreamingContext(sc,10)
lines = ssc.socketTextStream("localhost", 9999)
#lines = ssc.socketTextStream(...)  # create DStreams
#checkpointDirectory = "E:/USC/DataMining/Assignment/Assignment5/tmp"
checkpointDirectory = "./tmp12051993"
ssc.checkpoint(checkpointDirectory)  # set checkpoint directory
sc.setLogLevel(logLevel="OFF")

lim = 1000
val = math.log(lim,2)
window = OrderedDict()
numOfBuckets = int(val) + 1
listOfBucketSize = []
#for i in range(numOfBuckets):
i = 0
while i < numOfBuckets:
    value = (math.pow(2,i))
    window[int(value)] = []
    listOfBucketSize.append(int(value))
    i += 1
t = 0
u = 0
check = 1
actualCount = 0


initialCount = 0
actualList = ["999" for i in range(1000)]
#print("check a[786] %s" % actualList[786])
#print("len of actual list %d "% len(actualList))


# In[5]:


def func2(rddCollect):
    #s = time.time()
    global t,u,check,actualCount,initialCount,actualList
    for bit in rddCollect:
        bit = bit.encode("utf-8")
        tmp1 = t + 1
        t = (tmp1)%lim
        #print "check %d" % check
        if(initialCount != 1000):
                initialCount += 1
                
        for k in window.keys():
            if len(window[k]) == 0:
                break
            listOfBucket = window[k]
            for lsItem in listOfBucket:
                if lsItem == t:
                    #print "removing lsItem %d" % lsItem
                    
                    window[k].remove(lsItem)
                    #Reducing count of 1
                    #actualCount -= k
        arrIndex = u % 1000
        ch1 = actualList[arrIndex]
        if ch1 != "999":
            if ch1 == "1":
                if bit == "0":
                    actualCount -= 1
                    actualList.insert(arrIndex,bit)
            else:
                if bit == "1":
                    actualCount += 1
                    actualList.insert(arrIndex,bit)
        else:
            actualList.insert(arrIndex,bit)
            if bit == "1":
                actualCount += 1
        
        u += 1
        if bit == "1":
            
            #actualCount += 1
            window[1].append(t)
            #print "appending 1"
            #print window
            #Convert to list iif too slow
            for i in window.keys():
                if len(window[i]) == 0:
                    break
                for lsItem in window[i]:
                    if len(window[i]) > 2:
                        del window[i][0]
                        tmp = window[i][0]
                        del window[i][0]
                        lastItem = listOfBucketSize[-1]
                        if(i != lastItem):
                            index = 2 * i
                            window[index].append(tmp)
                        #print "after dict update"
                        #print window
                    else:
                        break
        #tmp2 = u + 1
        #u = tmp2%lim
        
    #if u == 0:
    #check that initially there should be 1000 bits in the window
    if initialCount == 1000:
        ptr = 0
        numOf1 = 0
        
        for key in window.keys():
            if len(window[key]) == 0:
                break
            if len(window[key]) > 0:
                
                ptr = window[key][0]
        for key in window.keys():
            if len(window[key]) == 0:
                break
            itemList = window[key]
            
            for i in itemList:
                if i == ptr:
                    numOf1 += int(key * (0.5))
                else:
                    numOf1 += key
        #print ("check = %d" % check)
        check += 1
        print ("Estimated number of ones in the last 1000 bits : %d " % numOf1)
        #print numOf1
        #print("\n")
        
        print("Actual number of ones in the last 1000 bits : %d " % actualCount)
        #print("\n")
        print("\n")
        #print(actualCount)
        #Commenting this out since we reduce value of actual count when we remove baskets
        #actualCount = 0
           
            
        #check += 1
        #e = time.time()
        #print e-s

		
def func(rdd):
    #print("size")
    
    val = rdd.collect()
   
    #print len(val)
    func2(val)


# In[7]:


words = lines.flatMap(lambda line: line.split(" "))
words.foreachRDD(func)
ssc.start()
ssc.awaitTermination()
