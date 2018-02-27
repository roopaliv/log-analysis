import glob
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib import style
#import Tkinter as tk
import numpy as np
import datetime as dt
from matplotlib.dates import DateFormatter, MinuteLocator



def my_int_conv(x):
    try:
        return int(x)
    except ValueError:
        return np.nan

def get_date(x):
    try:
        date_timestamp = dt.datetime.strptime(x,"%Y-%m-%dT%H:%M:%S.%fZ")
        #return dt.datetime(date_timestamp.year, date_timestamp.month, date_timestamp.day, date_timestamp.hour, 0, 0)
        #return dt.datetime(date_timestamp.year, date_timestamp.month, date_timestamp.day, 0, 0, 0)
        return dt.date(date_timestamp.year, date_timestamp.month, date_timestamp.day)
    except ValueError:
        return ""

def filter_unwanted_hosts(x):
    if x in wanted_hosts:
        return hosts_map[x]
    else:
        return "";

path =r'/home/rovij/Documents/new/zabbixLogs'
allFiles = glob.glob(path + "/logstash-*.csv")
frame = pd.DataFrame()
list_ = []
colNames = ["syslog_severity_code","syslog_facility_code","message","type","syslog_severity", "@timestamp","host"];
dtypes = {'syslog_severity_code':'int', 'syslog_facility_code':'int', 'message':'str','type':'str', 'syslog_severity':'str', '@timestamp':'str',  'host':'str'};
parse_dates = ['@timestamp']
for file_ in allFiles:
    print (file_)
    df = pd.read_csv(file_,error_bad_lines=False,  sep=',',  skipinitialspace=True,keep_default_na=False,
     usecols=colNames, header = 0,  converters={'syslog_severity_code':my_int_conv, 'syslog_facility_code':my_int_conv})
    #df = pd.read_csv(file_,error_bad_lines=False, header = 0, names=colNames, parse_dates=parse_dates, dtype=dtypes, sep=',',keep_default_na=False)
    list_.append(df)
frame = pd.concat(list_)
frame['@timestamp'] = frame['@timestamp'].apply(lambda x: get_date(x) if not pd.isnull(x) else '')
frame['host'] = frame['host'].apply(lambda x: filter_unwanted_hosts(x))
#print (frame.tail(10))]


#remove empty date_timestamp and hostnames

dropped_host_indexes = frame['host'].index[frame['host'].apply(lambda x: (x == "" ))]
frame.drop(frame.index[dropped_host_indexes], inplace=True)
dropped_date_indexes = frame['@timestamp'].index[frame['@timestamp'].apply(lambda x: x == "" or x == pd.NaT)]
frame.drop(frame.index[dropped_date_indexes], inplace=True)
frame['host'].replace('', np.nan, inplace=True)
frame.dropna(subset=['host'], inplace=True)
#print (frame['syslog_severity'].unique())
frame.rename(columns={'@timestamp': 'timestamp'}, inplace=True)
#print(list(frame)) #check columns in DataFrame

#print(frame[['message','timestamp', 'host']].groupby(['host','timestamp']).agg(['count']))
count = frame[['message','timestamp', 'host']].groupby(['host','timestamp']).agg(['count'])
#pd.DataFrame(count).to_excel('/home/rovij/PycharmProjects/AnalysisLogData/others_report.xls')
#pd.DataFrame(count).to_excel('/home/rovij/PycharmProjects/AnalysisLogData/hypervisors_report.xls')
pd.DataFrame(count).to_excel('/home/rovij/PycharmProjects/AnalysisLogData/storages_report.xls')

frame.pivot_table(values='message', index='timestamp', columns='host', aggfunc=len).plot(kind='bar')
#plt.show()
#plt.savefig('/home/rovij/PycharmProjects/AnalysisLogData/others_graph.png', bbox_inches='tight')
#plt.savefig('/home/rovij/PycharmProjects/AnalysisLogData/hypervisors_graph.png', bbox_inches='tight')
plt.savefig('/home/rovij/PycharmProjects/AnalysisLogData/storages_graph.png', bbox_inches='tight')
