#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: Jianmei Ye
@file: TopXSimpleLTV.py
@time: 4/30/17 8:27 PM
"""

import pandas as pd
import numpy as np
import datetime
import warnings

from dateutil import rrule


warnings.simplefilter(action = "ignore", category = RuntimeWarning)
class TopXSimpleLTV(object):
    def __init__(self,input_file = '../input/input.txt'):
        self.metric = pd.DataFrame(columns=['total_exp', 'total_visit',
                                            'exp_per_visit', 'visit_per_week', 'ltv'])
        self.filePath = input_file
        self.time_range = [None,None]
    def parseMixData(self,events = []):
        file_path = self.filePath
        firstLine = True
        with open(file_path) as f:
            for line in f.readlines():
                if firstLine:
                    firstLine = False
                    row = line.strip()[1:-1]
                else:
                    row = line.strip()[:-1]
                self.ingest(row,events)

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
        self.metric.sort_values(by='ltv', ascending=False)['ltv'][:x].\
                        to_json('../output/output.txt')
        print "Output generated!"




def Main(x=2):
    test = TopXSimpleLTV()
    test.parseMixData()
    test.topXSimpleLTVCustomers(x)


if __name__ == "__main__":
    Main()