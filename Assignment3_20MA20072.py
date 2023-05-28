from pyspark import SparkContext, SparkConf
import math
import sys
import logging
import re

if 'sc' in globals():
    sc.stop()
    

# Create a SparkContext
conf = SparkConf().setAppName("Assignment-3").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")

def main(file_path,target_word,k,stopword_filepath):
    # Read the input file into an RDD
    #file_path = "/Users/gunjan/Downloads/grade.txt"
    input_rdd = sc.textFile(file_path)
    
    # Define the target word
    #target_word = "risk"
    
    # Define the number of top co-occurring words to find
    #k = 5
    
    #loading the stop_word file 
    #stopword_filepath = "/Users/gunjan/Downloads/stopword-list.txt"
    stopword_rdd = sc.textFile(stopword_filepath)
    stopword = stopword_rdd.flatMap(lambda line: line.lower().split()).collect()
    
    #adding index to lines
    indexed = input_rdd.map(lambda x: re.sub(r'[^a-zA-Z]', ' ', x).lower()).zipWithIndex()
    N = indexed.count()
    lines = indexed.filter(lambda x: target_word in x[0])

    count = indexed.flatMap(lambda x: [(i,x[1]) for i in x[0].split() if i.isalpha()]).filter( lambda x: x[0] not in stopword).distinct()
    #lines.collect()
    words = lines.flatMap( lambda x: [(i,x[1]) for i in x[0].split() if i.isalpha()]).filter( lambda x: x[0] not in stopword).distinct()
    #count all occurences of words
    indexed.collect()

    count = count.countByKey()
    counts_rdd = sc.parallelize(list(count.items()))
    M = counts_rdd.lookup(target_word)
    #counts_rdd.collect()
    #words.collect()

    #count co-occurences of words
    words = words.filter(lambda x: x[0]!= target_word)
    common = words.groupByKey().map(lambda x: (x[0],len(x[1])))
    #common.collect()

    #join occurences and co-occurences
    final = common.join(counts_rdd)
    final.collect()

    #calculate score
    final = final.map(lambda x: (math.log2(N*int(x[1][0])/(int(x[1][1])*M[0])), x[0]))

    print("The list of k positively associated words: ")
    positive = final.sortByKey(False).take(k)
    for key,word in positive:
        print(word, key)

    print()
    print("The list of k negatively associated words: ")
    negative = final.sortByKey().take(k)
    for key,word in negative:
        print(word, key)

if __name__ == "__main__":
    file_path = sys.argv[1]
    target_word = sys.argv[2]
    k = int(sys.argv[3])
    stopword_filepath = sys.argv[4]
    main(file_path, target_word, k, stopword_filepath)
    #file_path = "/Users/gunjan/Downloads/grade.txt"
    #target_word = "efforts"
    #k = 10
    #stopword_filepath = "/Users/gunjan/Downloads/stopword-list.txt"
    #main(file_path,target_word,k,stopword_filepath)
    
sc.stop()