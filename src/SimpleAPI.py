#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
@author: Jianmei Ye
@file: SimpleAPI.py
@time: 4/30/17 7:48 PM
"""


from flask import Flask, jsonify, request
from werkzeug.serving import make_server
from TopXSimpleLTVForApi import *


app = Flask(__name__)
app.config["MONGO_DBNAME"] = "simpleltv_db"


@app.route("/topxsimpleltv", methods=['GET'])
def get():
    data = []
    for arg in request.args:
        if arg.lower() == 'x':
            x = request.args.get(arg)
            apicall = TopXSimpleLTVForApi()
            apicall.parseMixData()
            data = dict(apicall.topXSimpleLTVCustomers(int(x)))
    return jsonify({"Top "+x+" simple Lifetime Value ": data})

@app.route("/", methods=['POST'])
def post():
    data = request.get_json()
    if not data:
        data = {"response": "ERROR"}
        return jsonify(data)
    else:
        mongo = get_db()
        mongo.simpleLTV.drop()
        print "Start insert data to db..."
        mongo.simpleLTV.insert(data)
    return "OK"




if __name__ == "__main__":
    httpd = make_server('0.0.0.0', 5000, app, threaded=True)
    print "Serving http on port 5000..."
    httpd.serve_forever()
