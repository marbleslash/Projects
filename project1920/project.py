#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Mar 14 22:00:09 2020

@author: ufac001
"""
import numpy as np
from email.parser import Parser
import re
import time
from datetime import datetime, timezone, timedelta


from pyspark import SparkConf, SparkContext
from datetime import datetime, timezone


# Comment the following two lines out if using
# from Spark notebook
conf = SparkConf().setAppName("Enron")
sc = sc = SparkContext.getOrCreate(conf = conf)
sc.setLogLevel('WARN')


def date_to_dt(date):
    def to_dt(tms):
        def tz():
            return timezone(timedelta(seconds=tms.tm_gmtoff))
        return datetime(tms.tm_year, tms.tm_mon, tms.tm_mday,
                      tms.tm_hour, tms.tm_min, tms.tm_sec,
                      tzinfo=tz())
    return to_dt(time.strptime(date[:-6], '%a, %d %b %Y %H:%M:%S %z'))

# Q1: replace pass with your code
    ## Question 1)
def split_receivers(lst):
    #### The "To"
    if (type(lst[0]) == str):
        toos = re.sub("\s", "", lst[0])
        toos = toos.split(",")
    else:
        toos = []
    #### The "Cc"
    if (type(lst[1]) == str):
        ccs = re.sub("\s", "", lst[1])
        ccs = ccs.split(",")
    else:
        ccs = []
    if (type(lst[2]) == str):

        bccs = re.sub("\s", "", lst[2])
        bccs = bccs.split(",")
    else:
        bccs = []
    receivers =  toos + ccs + bccs
    return receivers


def extract_email_network(rdd):
  rdd_mail = rdd.map(Parser().parsestr)
  #val_by_vec
  val_by_vec = lambda x: [(yield(x[0], x[1][i], x[2])) for i in range(len(x[1]))]
  #Getting the senders and receivers
  rdd_full_email_tuples = rdd_mail.map(lambda x: (x.get('From'), split_receivers([x.get('To'), x.get('Cc'),  x.get('Bcc')]), date_to_dt(x.get('Date'))))
  rdd_email_triples = rdd_full_email_tuples.flatMap(val_by_vec)
  #email_regex
  email_regex = "[!#$%&'*+-/=?^_`{|}~\w]+@(\w+\.\w+)*\.{0,1}\w*[a-zA-Z]"
  #email_regex_match
  valid_email = lambda s: True if re.compile(email_regex).fullmatch(s) else False
  #checking if the email ends in "enron"
  enron_check = lambda s: True if re.compile('.+enron.com').fullmatch(s) else False
  rdd_email_triples_enron = rdd_email_triples.filter(lambda x: ( x[0] != x[1]) and (valid_email(x[0]) and valid_email(x[1])) and \
                                                    (enron_check(x[0]) and enron_check(x[1])) )
  return rdd_email_triples_enron.distinct()

# Expected output:
# ('george.mcclellan@enron.com', 'sven.becker@enron.com', 1)
# ('george.mcclellan@enron.com', 'stuart.staley@enron.com', 1)
# ('george.mcclellan@enron.com', 'manfred.ungethum@enron.com', 1)
# ('george.mcclellan@enron.com', 'mike.mcconnell@enron.com', 2)
# ('george.mcclellan@enron.com', 'jeffrey.shankman@enron.com', 2)
# ('stuart.staley@enron.com', 'mike.mcconnell@enron.com', 2)
# ('stuart.staley@enron.com', 'jeffrey.shankman@enron.com', 2)
# ('stuart.staley@enron.com', 'george.mcclellan@enron.com', 1)

# Q2: replace pass with your code
def convert_to_weighted_network(rdd, drange = None):
    if drange == None:
        distinct_dubles = rdd.map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y)
    else:
        d1 = drange[0]
        d2 = drange[1]
        distinct_dubles = rdd.filter(lambda x: x[2] >= d1 and x[2] <= d2).map(lambda x: ((x[0], x[1]), 1)).reduceByKey(lambda x, y: x + y)
    return distinct_dubles.map(lambda x: (x[0][0], x[0][1], x[1]))


# Q3.1: replace pass with your code
def get_out_degrees(rdd):
    ## OUT: Out_degrees for senders
    out = rdd.map(lambda x: (x[0], x[2])).reduceByKey(lambda x,y: x + y).map(lambda x: (x[1], x[0])).collect()
    ## OUT: out_degrees for people that didn't send
    senders = list(np.unique(rdd.map(lambda x: x[0]).collect()))
    not_out = rdd.filter(lambda x: x[1] not in senders).map(lambda x: (0, x[1])).distinct().collect()
    out_degrees_rdd = sc.parallelize(out + not_out)
    return out_degrees_rdd.sortBy(lambda x: (x[0], x[1]), ascending = False)

# Q3.2: replace pass with your code
def get_in_degrees(rdd):
    # IN : In_degrees for receivers
    in_deg = rdd.map(lambda x: (x[1], x[2])).reduceByKey(lambda x,y: x + y).map(lambda x: (x[1], x[0])).collect()
    ## IN: In_degrees for senders
    receivers = list(np.unique(rdd.map(lambda x: x[1]).collect()))
    not_in = rdd.filter(lambda x: x[0] not in receivers).map(lambda x: (0, x[0])).distinct().collect()
    in_degrees_rdd = sc.parallelize(in_deg + not_in)
    return in_degrees_rdd.sortBy(lambda x: (x[0], x[1]), ascending = False)


# Q4.1: replace pass with your code
def get_out_degree_dist(rdd):
    return get_out_degrees(rdd).map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+ y).sortBy(lambda x: x[0], ascending = True)


# Q4.2: replace pass with your code
def get_in_degree_dist(rdd):
    return get_in_degrees(rdd).map(lambda x: (x[0], 1)).reduceByKey(lambda x,y: x+ y).sortBy(lambda x: x[0], ascending = True)
