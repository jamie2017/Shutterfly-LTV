#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: Jianmei Ye
@file: TopXSimpleLTVForApi.py
"""
import warnings
import pandas as pd
import numpy as np
import datetime
from dateutil import rrule
from pymongo import MongoClient
import json,ast


def get_db():
    client = MongoClient('localhost:27017')
    db = client.simpleltv_db
    return db

warnings.simplefilter(action = "ignore", category = RuntimeWarning)
class TopXSimpleLTVForApi(object):
    def __init__(self):
        self.db = get_db()
        self.metric = pd.DataFrame(columns=['total_exp', 'total_visit',
                                            'exp_per_visit', 'visit_per_week', 'ltv'])
        self.time_range = [None,None]

    def parseMixData(self,events = []):
        eventList = list(self.db.simpleLTV.find({}, {'_id': False}))
        for e in eventList:
            n_e = ast.literal_eval(json.dumps(e))
            self.ingest(str(n_e), events)

    def ingest(self,e, data):
        e = eval(e)
        data.append(e)
        if e['type'] == 'IMAGE':
            return
        cid = ''
        event_date = datetime.datetime.strptime(str(e['event_time'])[:10], '%Y-%m-%d')
        if e['type'] == 'CUSTOMER':
            cid = e['key']
            if cid not in self.metric.index:
                self.metric.loc[cid] = [0.0, 0, 0, 0.0, 0.0]
        elif e['type'] == 'SITE_VISIT':
            cid = e['customer_id']
            if cid not in self.metric.index:
                self.metric.loc[cid] = [0.0, 1, 0, 0.0, 0.0]
            else:
                self.metric.loc[cid, 'total_visit'] += 1
        elif e['type'] == 'ORDER':
            cid = e['customer_id']
            expenditure = float(e['total_amount'][:-3])
            if cid not in self.metric.index:
                self.metric.loc[cid] = [expenditure, 0, 0, 0.0, 0.0]
            else:
                self.metric.loc[cid, 'total_exp'] += expenditure
        if self.time_range[0]:
            self.time_range[0] = min(self.time_range[0], event_date)
        else:
            self.time_range[0] = event_date
        if self.time_range[1]:
            self.time_range[1] = max(self.time_range[1], event_date)
        else:
            self.time_range[1] = event_date
        self.updateMetric(cid)

    def updateMetric(self,cid):
        if not cid:
            return
        num_weeks = self.countActiveWeeks(self.time_range[0],self.time_range[1])
        self.metric.loc[cid, 'exp_per_visit'] = round(self.metric.loc[cid]['total_exp'] /
                                                     self.metric.loc[cid]['total_visit'], 2)
        self.metric.loc[cid, 'visit_per_week'] = round(self.metric.loc[cid]['total_visit'] /
                                                      num_weeks, 2)
        self.metric.loc[cid, 'ltv'] = self.metric.loc[cid]['exp_per_visit'] * \
                                     self.metric.loc[cid]['visit_per_week'] * 52 * 10

    def countActiveWeeks(self,s,e):
        weeks = rrule.rrule(rrule.WEEKLY, dtstart=s, until=e)
        return weeks.count()

    def handleDivisorZero(self,x,y):
        return np.exp(np.log(x) - np.log(y))

    def topXSimpleLTVCustomers(self,x):
        return self.metric.sort_values(by='ltv', ascending=False)['ltv'][:x].iteritems()


    def generateDummyDataFromFile(self):
        # self.db.simpleLTV.drop()
        file_path = '../input/input.txt'
        firstLine = True
        with open(file_path) as f:
            for line in f.readlines():
                if firstLine:
                    firstLine = False
                    row = line.strip()[1:-1]
                else:
                    row = line.strip()[:-1]
                self.db.simpleLTV.insert(json.loads(row))

def Test(x=10):
    test = TopXSimpleLTVForApi()
    test.generateDummyDataFromFile()
    test.parseMixData()
    print type(test.topXSimpleLTVCustomers(x))
    print test.metric


if __name__ == "__main__":
    Test()


