
# coding: utf-8

# In[1]:

import os
import requests
from pprint import pprint
import json
from datetime import datetime,time
import glob
import time
import pandas as pd


# In[3]:

with open('config.json') as data_file:    
    data = json.load(data_file)



# In[ ]:




# In[4]:

import time
Date= time.strftime('%Y-%m-%d')


# In[5]:

Date


# In[6]:

file=data['state']+'_'+ Date+'_'+ '23155'+'.csv'


# In[7]:

new_file= os.getcwd()+'//'+file
new_file


# In[ ]:




# In[ ]:




# In[8]:

import io
import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])
bucket= s3.Bucket('adsassign1_databucket')
client= boto3.client('s3', 
                     aws_access_key_id=data["AWSAccess"],
                    aws_secret_access_key=data["AWSSecret"])
b= list(bucket.objects.all())
l=[(k, k.last_modified) for k in b]
l1= [k for k, v in sorted(l, key= lambda p: p[1], reverse=True)]
key_to_download=l1[0].key
a=key_to_download[3:13]
if os.path.exists(new_file):
    print(file+' exists')
else:
    df_temp=[]
    s3.Bucket('adsassign1_databucket').download_file(key_to_download, key_to_download)
    df=pd.read_csv(key_to_download)
    df_temp.append(df)
    s=requests.get(data['link']).content
    c=pd.read_csv(io.StringIO(s.decode('utf-8')))
    c1=pd.DataFrame(c)
    c1['new_date'] = c1.DATE.map(lambda x: str(x)[0:10])
    df1= c1.loc[(c1['new_date']>a)]
    df_temp.append(df1)
    full_df=pd.concat(df_temp)
    full_df.to_csv(new_file)


# In[9]:

import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])


# In[10]:

uploadFileNames = []

for filename in glob.glob("*.csv"):
    uploadFileNames.append(filename)
    print(filename)


# In[ ]:




# In[ ]:




# In[ ]:

import boto3
import botocore

s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])
exists= False
for files in uploadFileNames:
    try:

            s3.Object('adsassign1_databucket', files).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            s3.Object("adsassign1_databucket", files).put(Body=open(files, 'rb'))
            print(files)
            
        else:
            raise
    else:
        exists = True

    print(files+''+' exists')


# In[ ]:




# In[ ]:



