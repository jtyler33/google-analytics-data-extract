from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import json
import os
from datetime import date, datetime, timedelta
from dateutil.rrule import rrule, DAILY
import dateutil.parser
from time import sleep
import boto3
import pandas as pd

def initialize_analyticsreporting():
        SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
        KEY_FILE_LOCATION = 'file.json'
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            KEY_FILE_LOCATION, SCOPES)
    
        # Build the service object.
        analytics = build('analyticsreporting', 'v4', credentials=credentials)
    
        return analytics
    
    
def get_report(analytics, DT):
        VIEW_ID = '123456789' #global view
        return analytics.reports().batchGet(
            body={
                'reportRequests': [
                    {
                        'viewId': VIEW_ID,
                        'pageSize': 100000,
                        'dateRanges': [{'startDate': DT, 'endDate': DT}],
                        'metrics': [{'expression': 'ga:pageviews'}, {'expression': 'ga:entrances'},
                                    {'expression': 'ga:timeOnPage'},
                                    {'expression': 'ga:pageLoadTime'}, {'expression': 'ga:searchDuration'},
                                    {'expression': 'ga:users'},
                                    {'expression': 'ga:newUsers'}, {'expression': 'ga:sessions'},
                                    {'expression': 'ga:bounces'}, {'expression': 'ga:sessionDuration'}],
                        'dimensions': [{'name': 'ga:dateHourMinute'}, {'name': 'ga:channelGrouping'},
                                       {'name': 'ga:sourceMedium'}, {'name': 'ga:hostname'}, {'name': 'ga:pagePath'}, 
                                       {'name': 'ga:deviceCategory'}, {'name': 'ga:pageTitle'}]
                    }]
            }
        ).execute()
    
def name_extract(dictionary):
        extract = dictionary.get('name')
        return extract        
    
def replacer(string):
        string = string.replace(':', "_")
        return string        

def parseResponse(report):
        report = report.get('reports')
        data_list = []
        for dic in report:
            dimHeader = dic.get('columnHeader').get('dimensions')
            dimHeader = list(map(replacer, dimHeader))
            metricHeader = dic.get('columnHeader').get('metricHeader').get('metricHeaderEntries')
            metricHeader = list(map(name_extract, metricHeader))
            metricHeader = list(map(replacer, metricHeader))
            
            data = dic.get('data').get('rows')
            if data is not None:
                for subdic in data:
                    dims = subdic.get('dimensions')
                    dims_list = list(zip(dimHeader, dims))
                    metrics = subdic.get('metrics')
                    for subsubdic in metrics:
                        metrics_data = subsubdic.get('values')
                        metrics_list = list(zip(metricHeader, metrics_data))
                        full_row = dims_list + metrics_list
                        data_list.append(full_row)
            else:
                pass
                    
        data_list = list(map(dict, data_list))
        
        return data_list

def main():
        #INITIATE S3 CONNECTION OBJECTS
        BUCKET_NAME = 'Bucket'
        s3 = boto3.resource('s3')
        bucket = s3.Bucket(BUCKET_NAME)
        
        #DOWNLOAD GOOGLE KEY FILE
        local_key_file_name = 'file.json'
        KEY_KEY_IN = 'Path/to/file.json'
        s3.Bucket(BUCKET_NAME).download_file(KEY_KEY_IN, local_key_file_name)
        
        #DOWNLOAD OLD TIMESTAMP FILE
        local_txt_file_name = 'GOOGLE_Timestamp_GA.txt'
        KEY_TXT_IN = 'Path/to/GOOGLE_Timestamp_GA.txt'
        s3.Bucket(BUCKET_NAME).download_file(KEY_TXT_IN, local_txt_file_name)
        
        #READ IN OLD TIMESTAMP
        file2 = open("GOOGLE_Timestamp_GA.txt", "r")
        last_updated = file2.read()
        file2.close
        
        #EXTRACT THE Y, M, D FROM OLD TIMESTAMP
        last_updated = last_updated.split('-')
        y = int(last_updated[0])
        m = int(last_updated[1])
        d = int(last_updated[2])
        
        analytics = initialize_analyticsreporting()
        b = date(y, m, d)
        a = b - timedelta(days = 1)
        today = datetime.today()
        
        file = []
        for dt in rrule(DAILY, dtstart=a, until=b):
            response = get_report(analytics, dt.strftime("%Y-%m-%d"))
            file.append(response)
            sleep(1) 
        
        file = list(map(parseResponse, file))
        
        file = [subdic for dic in file for subdic in dic]
        
        file = pd.DataFrame(file)

        #UPDATE DATA
        file.to_csv('GA_STATS.csv', index = False, header = True)
        
        #REPLACE OLD TIMESTAMP WITH NEW TIMESTAMP
        with open('GOOGLE_Timestamp_GA.txt', 'w') as time_file:
            time_file.write(today.strftime("%Y-%m-%d"))
            
        KEY = 'Path/to/GA_STATS.csv'
        KEY_TXT = 'Path/to/GOOGLE_Timestamp_GA.txt'
        
        bucket.upload_file('GA_STATS.csv', KEY)
        bucket.upload_file('GOOGLE_Timestamp_GA.txt', KEY_TXT)

        time_file.close

if __name__ == '__main__':
        main()
