import tarfile
import os
import re
import pandas as pd
from itertools import islice
import sys

path1 = input("Enter path:")

import sys

#reading input
#path = r'D:\Nidhi\MA\MA\SEM-6\BDP\data2.txt'
path1 = sys.argv[1]
t = sys.arg[2]
n = sys.arg[3]
k = sys.arg[4]
X = pd.read_csv(path, sep=" ", header=None)

#path = r"C:/Users/Nidhi/Downloads"
#os.chdir(path1)
#filename = "20_newsgroups.tar.gz"
tf = tarfile.open(path1, mode = "r:gz")
index =tf.getnames() 
members = tf.getmembers()

y = []
files = {}
for x in index:  
    if ("/" in x) &(len(x.split("/"))>2) :
        files[x] = x.split("/")[1]
        y.append(x.split("/")[1])
file_names = list(files.keys())

count = pd.Series(y).value_counts()
folder_names = list(count.index)

def generate_ngrams(text, WordsToCombine):
     words = text.split()
     output = []  
     for i in range(len(words)- WordsToCombine+1):
         output.append(' '.join(words[i:i+WordsToCombine]))
     return output


results = []
def map1(file_name, start, end, n):
    for i in range(start,end):
        z = file_name[i]
        class_label = z.split("/")[1]
        #print(class_label)
        w = open(path1+ "/extraction_dir/"+z, "r", encoding = "windows-1252")
        string = w.read()
        #string = z.read().decode("ascii").lower()
        s = re.sub('[^0-9a-zA-Z]+', ' ', string)
        n_grams_count = pd.Series(generate_ngrams(s, 3)).value_counts()
        n_grams_list = pd.Series(n_grams_count.index)
        label=pd.Series([class_label]*len(n_grams_count))
        #results.extend([[i,[a,b]] for i, a,b in zip(n_grams_list, label, n_grams_count)])
        for i in range(len(label)):
            results.append([n_grams_list[i],[label[i],n_grams_count[i]]])

intermediate_results = {}
def groupByKey(data, start, end):
    #result = dict()
    for i in range(start, end):
        x = data[i]
        key = x[0]
        value = x[1]
        if key in intermediate_results:
            intermediate_results[key].append(value)
            #print(key, intermediate_results[key])
        else:
            intermediate_results[key] = value
            #print(key, intermediate_results[key])
       
n_gram_score = {}
def reducer(intermediate_results, start, end):
    for key, value in islice(intermediate_results.items(), start, end):
        freq = {}
        for k in folder_names:
            freq[k] = 0
        if (intermediate_results[key][0] in list(freq.keys())):
                freq[intermediate_results[key][0]]+=intermediate_results[key][1]
        if((intermediate_results[key][0] in freq) & (intermediate_results[key][0] in folder_names)):
            freq[intermediate_results[key][0]] = intermediate_results[key][1]
        max = 0
        class_salience_score = 0
        for k in folder_names:
            #print(freq[intermediate_results[key][0]])
            class_salience_score = (freq[intermediate_results[key][0]]/count[intermediate_results[key][0]])
            if (class_salience_score > max):
                max = class_salience_score
        n_gram_score[key] = max

i=0

from threading import Thread

threads = [None] * t

size = int(len(files)/len(threads)) # number of maps
for i in range(len(threads)):
    start = i*size
    end = (i+1)*size
    threads[i] = Thread(target=map1, args=(list(files.keys()), start, end, n))
    threads[i].start()
    
for i in range(len(threads)):
    threads[i].join()
    
threads = [None] * t
size = int(len(results)/len(threads))


for i in range(len(threads)):
    start = i*size
    end = (i+1)*size
    threads[i] = Thread(target=groupByKey, args=(results, start, end))
    threads[i].start()

for i in range(len(threads)):
    threads[i].join()


threads = [None] * t
size = int(len(intermediate_results)/len(threads))


for i in range(len(threads)):
    start = i*size
    end = (i+1)*size
    threads[i] = Thread(target=reducer, args=(intermediate_results, start, end))
    threads[i].start()

for i in range(len(threads)):
    threads[i].join()    

sorted_list = sorted(n_gram_score.items(), key = lambda x:x[1], reverse =True)


for i in range(k):
    print(sorted_list[i][0], ":", sorted_list[i][1])
