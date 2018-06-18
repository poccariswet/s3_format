import boto3
from boto3 import Session
import os
from datetime import datetime, timedelta
import time

start_time = time.time()

s3 = boto3.client('s3')
start = datetime.strptime('20161115', '%Y%m%d')
end = datetime.strptime('20180615', '%Y%m%d')

def daterange(start_date, end_date):
    for n in range((end_date - start_date).days):
        yield start_date + timedelta(n)

dates = []
for i in daterange(start, end):
    y = str(i.year)
    m = str(i.month)
    d = str(i.day)
    if len(m) !=2:
        m = '0' + m
    if len(d) != 2:
        d = '0' + d

    dates.append(y+m+d)


s3client = Session().client('s3')
rdata=[]

for date in dates:

    response = s3client.list_objects(
        Bucket='ld-rawdata-2',
        Prefix= 'TR_JISSEKI/' + date + 'XXXXXX/'
    )

    if 'Contents' in response:  # 該当する key がないと response に 'Contents' が含まれない
        keys = [content['Key'] for content in response['Contents']]
        key = keys[-1] #２３時のデータ


    records = b""

    sql = "SELECT s._1,s._2,s._4,s._5,s._9,s._12,s._13 FROM s3Object as s where (s._2=\'0001\' or s._2=\'0048\' or s._2=\'0052\') and (s._4=\'9263126700000\' or s._4=\'9261821300000\' or s._4=\'9262702400000\' or s._4=\'9285130600000\' or s._4=\'9264112900000\' or s._4=\'9264102000000\' or s._4=\'9264103700000\' or s._4=\'9261106100000\' or s._4=\'9263621700000\' or s._4=\'9262543300000\' or s._4=\'9263125000000\' or s._4=\'9265625300000\' or s._4=\'9264904000000\' or s._4=\'9261151100000\' or s._4=\'9266505700000\' or s._4=\'9285106100000\' or s._4=\'9265661100000\' or s._4=\'9266339800000\' or s._4=\'9262102200000\' or s._4=\'9261808400000\' or s._4=\'9285108500000\' or s._4=\'9264545500000\' or s._4=\'9262201200000\' or s._4=\'9286901100000\' or s._4=\'9261105400000\' or s._4=\'9264513400000\' or s._4=\'9265603100000\' or s._4=\'9262902800000\' or s._4=\'9263620000000\' or s._4=\'9264514100000\')"

    r = s3.select_object_content(
        Bucket="ld-rawdata-2",
        Key=key,
        ExpressionType='SQL',
        Expression=sql,
        InputSerialization = {'CSV': {"FileHeaderInfo": "IGNORE"}},
        OutputSerialization = {'JSON': {}},
        )

    for event in r['Payload']:
        if 'Records' in event:
            records += event['Records']['Payload']
        rdata.append(records.decode('utf-8', 'replace'))
        records=b""

#format_csv_3Dlist = [[x.split(',') for x in data] for data in [x.split('\n') for x in rdata]] #split csv , \n


print(rdata)

elapsed_time = time.time() - start_time
print ("elapsed_time:{0}".format(elapsed_time) + "[sec]")
