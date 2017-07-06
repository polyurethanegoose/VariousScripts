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

try:
    import pika
except ImportError:
    print('''\033[91mERROR!\033[0m
It looks like there's a problem with importing \033[94mpika\033[0m library.
Probably it's not installed in the system. Please try installing it by:

  \033[93mpip install pika\033[0m
     or
  \033[93mapt-get install python-pika\033[0m

You can find more info on:
 * https://github.com/pika/pika
 * https://pika.readthedocs.io/

Script will now quit...''')
    quit()

LDATE = datetime.now()-timedelta(hours=1)
LDATE = LDATE.strftime('%Y-%m-%dT%H')

REGL = re.compile(LDATE+"\:[0-9]{2}\:[0-9]{2}.*$") # Messages from specified hour
REGS = re.compile(LDATE+"\:[0-9]{2}\:[0-9]{2}.*status=(deferred|bounced).*$")
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
    frm = search(mid) # Sender address
    if 'status=deferred' in dat:
        msg = "deferred-message"
        xch = "x.postfix-analyser.deferred-message"
    elif 'status=bounced' in dat:
        msg = "bounced-message"
        xch = "x.postfix-analyser.bounced-message"
    rabbit(mid, msg, frm, rec, tsp, xch)

def search(mid):
    '''
    Searches through captured lines to find message sender.
    '''
    reg = re.compile('^.*{}.*from=.*$'.format(mid))
    mat = filter(reg.match, LNS)
    spl = re.split("\ ", mat[0])
    return spl[4].replace("from=<", "").replace(">,", "")

def rabbit(mid, typ, frm, rec, dte, exc):
    '''
    Showels data into rabbit.
    '''
    pld = {"id":mid,
           "type":typ,
           "retry_attempts":0,
           "payload":{"sender_email":frm,
                      "recipient_email":rec,
                      "timestamp":dte}}
    crd = pika.PlainCredentials('user', 'password')
    con = pika.BlockingConnection(pika.ConnectionParameters(
        host='rabbit.prod',
        virtual_host="bounces",
        credentials=crd))
    chn = con.channel()
    try:
        chn.basic_publish(exchange=exc,
                          routing_key='TEST',
                          body=json.dumps(pld),
                          properties=pika.BasicProperties(
                              content_type="application/json",
                              delivery_mode=2)
                         )
    except Exception as err:
        print(err)
    finally:
        con.close()

LNS = []
for line in open("/var/log/mail.log"):
    if re.match(REGL, line):
        LNS.append(line)
        if re.match(REGS, line) and not re.match(REGR, line):
            parser(line)
