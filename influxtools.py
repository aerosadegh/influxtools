#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 28 08:22:43 2019

@author: tt
"""

from datetime import datetime, timedelta
import numpy as np
from influxdb import InfluxDBClient, DataFrameClient
import pandas as pd
import os
import re
import requests


# release Date: 17-04-99
__version__ = 0.4


client = InfluxDBClient(host='localhost', port=8086)
clientdf = DataFrameClient(host='localhost', port=8086)

client.switch_database('dbname')
clientdf.switch_database('dbname')


def create_switch_database(dbname):
    client.create_database(dbname)
    client.switch_database(dbname)
    clientdf.switch_database(dbname)



def datetime_from_str(str):
    return datetime.strptime(str, "%Y-%m-%dT%H:%M:%S.%fZ")

def time2str(time):
    return time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def q2df(res):
    return [pd.DataFrame(data=c[1]) for c in res.items()][0]    


def str2timestanp(st):
    try:
        return int((datetime.strptime(st, "%Y-%m-%dT%H:%M:%S.%fZ") + timedelta(hours=4.5)).timestamp())
    except:
        return int((datetime.strptime(st, "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=4.5)).timestamp())
    
    
def timestamp2str(timestamp):
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ")


def read_influx(measurement):
    print("Get all Data...")
    df = q2df(client.query(f"select * from {measurement}"))

    print("Convert & Create Timestamp...")
    df["timestamp"] = df["time"].apply(str2timestanp)
    return df



def write_df(df, measurment, values_header, tags_header, time_header="timestamp", filename=None, database="dbname", insert_data=False):
    """v0.2 time_header default to second resolution


    measurment = "measur"
    values_header= []  # or Dict
    tags_header = []     # or Dict
    time_header = "timestamp"   # Default to "timestamp"
    
    values_header= {}  # or List 
    tags_header = {}     # or List
    """
    
    try:
        values_header.remove(time_header)
    except:pass
    try:
        tags_header.remove(time_header)
    except:pass
    
    st = f"{measurment}"
    
    for tc in tags_header:
        if isinstance(tags_header, dict):
            st += f",{tags_header[tc]}=" + '{tc}'.replace("tc", tc) 
        else:
            st += f",{tc}=" + '{tc}'.replace("tc", tc)
        
    
    
    st += " "
            
    for vc in values_header:
        if isinstance(values_header, dict): 
            st += f"{values_header[vc]}=" + '{vc},'.replace("vc", vc)
        if isinstance(values_header, list) or isinstance(values_header, tuple):
            st += f'{vc.replace("-", "_")}=' + '{vc},'.replace("vc", vc)
    
    
           
    st = st[:-1] + ' {'+time_header+':.0f}\n'
    
    
    line = f"""# DDL

CREATE DATABASE {database}

# DML

# CONTEXT-DATABASE: {database}\n\n"""
    
    print("ST:" , st)

    
    regex = r"(\w+\=\{(\w+)(?:!r)?\})"    
    matches = re.finditer(regex, st)
    dd = dict()
    for match in matches:
        dd.update(dict([tuple(match.groups())[::-1]]))

    for row in df.iterrows():
        stt = st
        r = row[1]
        #print("R:", r.index)
        #print("DD:", dd)
        for key in r.index:
            tt = r.get(key)
            #print(type(tt))
            if type(tt) in [float, int, complex] :
                #print(type(tt))
                if np.isnan(tt):
                    stt = stt.replace(dd.get(key,""), "").replace(",,", ',').replace(', ', ' ').replace(' ,', ' ')
                    #print(stt)
        #print(r)
        line += stt.format(**r)
    #print("stt:", stt, "\n")
    #print("line:", line[-1000:], "\n")    
    if filename:
        newline = None
        if os.name == "nt":
            newline = "\n"
        with open(filename, "w", newline=newline) as f:
            res = f.write(line)
        #ans = input("Are You Sure To WRITE DATA?[y/n]: ")
        #if ans.lower() == "y":
        #os.system(f"influx -import -path={filename} -precision=s")


        return res
    else:
        return line




def databases_show():
    res = clientdf.get_list_database()
    for i, c in enumerate(res):
        print(f"DB_{i+1:02}: ", c["name"])


def measurements_show():
    res = clientdf.get_list_measurements()
    for i, c in enumerate(res):
        print(f"MS_{i+1:02}: ", c["name"])



def read_dataframe(measurement, limit=1000):
    """Return a DataFrame 
    limit: 0 for all points"""
    res = clientdf.query(f"select * from {measurement} limit {limit}")
    return res[measurement]

def write_dataframe(df, measurement):
     clientdf.write_points(df, measurement)








