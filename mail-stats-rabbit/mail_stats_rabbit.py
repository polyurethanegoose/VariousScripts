#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
Reads mail logs from specified time and pushes data into rabbit queue.
'''
from __future__ import print_function
from datetime import datetime, timedelta
import time
import re
import json
import pika

LDATE = datetime.now()-timedelta(hours=1)
LDATE = LDATE.strftime('%Y-%m-%dT%H')

REGL = re.compile(LDATE+"\:[0-9]{2}\:[0-9]{2}.*status=(deferred|bounced).*$")
REGR = re.compile("relay=local")

def parser(dat):
    '''
    Reads and parses lines.
    '''
    dat = re.split("\ ", dat)
    tsp = dat[0].replace(' ', '')[:-6] # Remove UTC offset...
    tsp = datetime.strptime(tsp, "%Y-%m-%dT%H:%M:%S.%f") # ...format date...
    tsp = int(time.mktime(tsp.timetuple())) # ...and convert to epoch.
    rec = dat[4].replace("to=<", "").replace(">,", "") # Recipent address
    mid = dat[3].replace(":", "") # Message ID
    if 'status=deferred' in dat:
        msg = "deferred-message"
        xch = "x.postfix-analyser.deferred-message"
    elif 'status=bounced' in dat:
        msg = "bounced-message"
        xch = "x.postfix-analyser.bounced-message"
    rabbit(mid, msg, rec, tsp, xch)

def rabbit(mid, typ, rec, dte, exc):
    '''
    Showels data into rabbit.
    '''
    pload = {"id":mid,
             "type":typ,
             "retry_attempts":0,
             "payload":{"sender_email":"None",
                        "recipient_email":rec,
                        "timestamp":dte}}
    cred = pika.PlainCredentials('user', 'password')
    conn = pika.BlockingConnection(pika.ConnectionParameters(
        host='rabbit.prod',
        virtual_host="bounces",
        credentials=cred))
    chnl = conn.channel()
    try:
        chnl.basic_publish(content_type="application/json",
                           exchange=exc,
                           body=json.dumps(pload),
                           properties=pika.BasicProperties(
                               delivery_mode=2)
                          )
    except Exception as err:
        print(err)
    finally:
        conn.close()

for line in open("/var/log/mail.log"):
    if re.match(REGL, line) and not re.match(REGR, line):
        parser(line)
