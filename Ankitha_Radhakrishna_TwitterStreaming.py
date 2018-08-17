from __future__ import absolute_import, print_function
import tweepy
# Import modules
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
#import dataset
from sqlalchemy.exc import ProgrammingError
import json
import random
import operator
import time
import math

consumer_secret = "pvxwfsQEIi3imahzXrRnSdtW1VoqMdLmJDRevQ01OYmYrTE63I"
consumer_key = "kGZ3akuYxIiSoAZxKTo8p6nYv"
access_token_secret = "8jFM24kC38HBsAkWrSa7VRqF811dV927vVHPhDgeE1o1Y"
access_token = "984326592305377280-EOtkpa0kmideQmzRDiDM0CfRJuI1ZPQ"


class StdOutListener(StreamListener):
    def __init__(self,a,b,c,d,e):
        self.sumAvg = a
        self.dictHashTags = b
        self.listOfTweets = c
        self.dictTweets = d
        self.count = e
    
    def on_error(self, status_code):
        if status_code == 420:
            return False
        
    def on_data(self, raw_data):
        data = json.loads(raw_data)
        #print("**************************on_data****************************")
        #print("self.count")
        #print(self.count)
        if('text' in data.keys()):
            text = ""
            lenOfText = ""
            if('extended_tweet' in data.keys()):
                text = data['extended_tweet']['full_text']
                lenOfText = len(text)
                #print(data['extended_tweet']['full_text'])
                #print(len(data['extended_tweet']['full_text']))
            else:
                text = data["text"]
                lenOfText = len(text)
                #print(data["text"])
                #print(len(data["text"]))
        hashtags = []  
        if('entities' in data.keys()):
            if('hashtags' in data['entities']):
                hashtags = data['entities']['hashtags']
                #print(data['entities']['hashtags'])
                
        #Change to 100
        if(self.count < 100):
            #print("IF")
            self.count += 1
            self.sumAvg += lenOfText
            #listOfTweets.append(text)
            #dictTweets[self.count] = lenOfText
            listOfHashTags = []
            for each in hashtags:
                #hashName = str(each['text'])
                hashName = each['text'].encode('utf-8')
                #print("hashName")
                #print(hashName)
                listOfHashTags.append(hashName)
                if hashName in dictHashTags.keys():
                    dictHashTags[hashName] += 1
                else:
                    dictHashTags[hashName] = 1
            
            dictTweets[self.count] = [lenOfText,listOfHashTags]
            #print("dictTweets")
            #print(dictTweets)
                
        else:
            #print("ELSE")
            #print(dictTweets)
            self.count += 1
            self.sampling(self.count,lenOfText,hashtags)
            #print("Returned from sampling")
            
            #print(dictHashTags)
        
    def sampling(self,n,lenOfText,hashTags):
        
        r = random.randint(1,n)
        #print("r")
        #print(r)
        #Change to r > 100
        if r > 100:
            return
        else:
            oldLen = self.dictTweets[r][0]
            newLen = lenOfText
            #print("newLen")
            #print(newLen)
            self.dictTweets[r][0] = newLen
            self.sumAvg = self.sumAvg - oldLen + newLen
            #Change to 100
            avg = float(self.sumAvg)/100
            #print("avg")
            #print(avg)
            #reduce count of existing hash tags
            existingHashTags = self.dictTweets[r][1]
            #print("existingHashTags")
            #print(existingHashTags)
            for i in existingHashTags:
                #print("i")
                #print(i)
                self.dictHashTags[i] -= 1
                if self.dictHashTags[i] == 0:
                    self.dictHashTags.pop(i)
            #replace hashtag value in dictTweets and increase/add value in dicthashTags
            listOfHashTags = []
            for each in hashTags:
                #hashName = str(each['text'])
                hashName = each['text'].encode('utf-8')
                listOfHashTags.append(hashName)
                if hashName in dictHashTags.keys():
                    self.dictHashTags[hashName] += 1
                else:
                    self.dictHashTags[hashName] = 1
            self.dictTweets[r][1] = listOfHashTags
            #print("Hashtags")
            #print(dictHashTags)
            top5 = list(sorted(self.dictHashTags.iteritems(), key=operator.itemgetter(1), reverse=True)[:5])
            #print("top5")
            #print(top5)
            self.printDetails(n,top5,avg)
            
    def printDetails(self,n,listTags,avg):
        #f = open("E:/USC/DataMining/Assignment/Assignment5/tmp/AnkithaRadhakrishna_Task5.txt",'a')
        f = open("AnkithaRadhakrishna_DGIMAlgorithm.txt",'a')
        print("The number of twitter from beginning: %d" % n)
        line = "The number of twitter from beginning:"+str(n)+"\n"
        print("Top 5 hashtags:")
        line+="Top 5 hashtags:\n"
        for i in listTags:
            print("%s:%d" % (i[0],i[1]))
            line+=i[0]+":"+str(i[1])+"\n"
        #avgCheck = math.ceil(avg)
        print("The average length of twitter is: %s\n" % str(round(avg,2)))
        
        line+="The average length of twitter is: "+ str(round(avg,2))+"\n\n"
        f.write(line)
        f.close()
        


# In[ ]:


if __name__ == '__main__':
    s = time.time()
    sumAvg = 0
    dictHashTags = {}
    listOfTweets = []
    dictTweets = {}
    count = 0
    l = StdOutListener(sumAvg,dictHashTags,listOfTweets,dictTweets,count)
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    stream = Stream(auth, l)
    #stream.filter(track=['a','the','i'])
    stream.filter(track=['california'])

