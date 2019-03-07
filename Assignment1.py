######################################################
## Assignment 1 - Word Count Using PySpark          ##
## Author       - Akshay Patel                      ##
## Usage        - Copy paste all code in pyspark    ##
##                until graph is rendered in window ##
######################################################


import matplotlib.pyplot as plt
from string import punctuation
from operator import add
import numpy as np
import re
import time


################ Word Count Application On Shakespeare's File #################
def wordCount(wordListRDD):
    """
    Creates a pair RDD with word counts from an RDD of words.
    
    Args:
        wordListRDD (RDD of str): An RDD consisting of words.
    Returns:
        RDD of (str, int): An RDD consisting of (word, count) tuples.
    """
    wordPairs = wordListRDD.map(lambda w: (w, 1))
    wordCounts = wordPairs.reduceByKey(add)
    
    return wordCounts


#ToDO : Check later if performance issue
def removePunctuation(text):
    """
    Removes punctuation, changes to lower case, and strips leading and trailing spaces.
    Note:
        Only spaces, letters, and numbers should be retained. Other characters should be eliminated (e.g. it's becomes its). Leading and trailing spaces should be removed after punctuation is removed.
    Args:
        text (str): A string.
    Returns:
        str: The cleaned up string.
    """
    #Remove puctuations from text. Predefined punctuations available in puctuation module
    removedPunctuationStr = ''.join(char for char in text if char not in punctuation)
    
    #Remove extra spaces
    removedSpaces = re.sub('  *', ' ', removedPunctuationStr)
    
    #Remove spaces from beginning and end of text String
    removedSpaces2 = removedSpaces.strip()
    
    #Make all text strings to lower case
    cleanText = removedSpaces2.lower()
    
    return cleanText


## For perf check ##
start = time.time()

## Load the of data file
filename = "shakespeare.txt"
shakespeareRDD = sc.textFile(filename, 8).map(removePunctuation)

## Print some lines of strings ##
print('\n'.join(shakespeareRDD.zipWithIndex().map(lambda l: '{0} :{1}'.format(l[0],l[1])).take(15)))

## Split the strings into separate words ##
shakespearWordsRDD = shakespeareRDD.flatMap(lambda s: s.split(' '))
shakespearWordsCount = shakespearWordsRDD.count()
print(shakespearWordsCount)


## Remove all blanks ##
shakespearWordsRDD2 = shakespearWordsRDD.filter(lambda y:  y != '')
print(shakespearWordsRDD2.count())

## Get top 20 words in descending order of their word count in corpus ##
top15Words = wordCount(shakespearWordsRDD2).takeOrdered(15, lambda s: s[1]* -1)
print('\n'.join(map(lambda w: '{0}: {1}'.format(w[0], w[1]), top15Words)))


## Check Execution time #############################
end = time.time()
exec_time = end - start
print("{0} ms".format(exec_time * 1000))


##### Plot Histogram using matplotlib ###############
xlist = []
ylist = []
for x,y in top15Words:
    xlist.append(x)
    ylist.append(y)

nums = np.arange(15)
plt.bar(nums, ylist)
plt.xticks(nums, xlist)
plt.show()

