
# coding: utf-8

# In[1]:

import os
import requests
from pprint import pprint
import json
from datetime import datetime,time, timedelta
import glob
import time
import pandas as pd
import logging
import urllib.request
import logging
import logging.handlers
import boto3
import botocore
import botocore.session


# In[ ]:




# In[2]:

import logging
import logging.handlers

logger=logging.getLogger(__name__)
logger.setLevel(logging.INFO)

logfile1 = time.strftime("%Y-%m-%d_%H_%M_%S"+".log")
print (logfile1)
handler= logging.FileHandler(logfile1)
handler.setLevel(logging.INFO)

formatter= logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.info('hello ')



# In[ ]:




# In[3]:

logger.info('Reading Json')
with open('intial_config.json') as data_file:    
    data = json.load(data_file)

# print(data)


# In[4]:

links=[]
for values in (data['link1'],data['link2'],data['link3'],data['link4'],data['link5'],data['link6'],data['link7'],data['link8']):
               links.append(values)
links


# In[5]:

# path=os.getcwd()
# #os.mkdir('Data1')
# final_path= path+'/'+'Data1/'


# In[6]:

# final_path


# In[7]:

import io
df_list= []

for x in links:
    
    s=requests.get(x).content
    c=pd.read_csv(io.StringIO(s.decode('utf-8')))
    df_list.append(c)
full_df=pd.concat(df_list)


# In[8]:

# full_df.to_csv(os.path.join(final_path,'CA_140617_23155.csv'))


# In[9]:

full_df.to_csv(('CA_2017-06-14_23155.csv'))


# In[10]:

full_df.head(2)


# In[ ]:


        
        


# In[17]:

import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])

client= boto3.client('s3', 
                     aws_access_key_id=data["AWSAccess"],
                    aws_secret_access_key=data["AWSSecret"])


# In[18]:

# s3.create_bucket(Bucket='adsassign1_databucket')


# In[19]:

names=[]
response = client.list_buckets()
for bucket in response["Buckets"]:
    names.append(bucket)

Bucketname= 'adsassign1_databucket'
if Bucketname in names:
    print('it exists')
else:
    s3.create_bucket(Bucket=Bucketname)   


# In[20]:

uploadFileNames = []

for filename in glob.glob("*.csv"):
    uploadFileNames.append(filename)
    print(filename)


# In[21]:

import botocore.session
for files in uploadFileNames:
    try:
        s3.Object('adsassign1_databucket', files).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            s3.Object("adsassign1_databucket", files).put(Body=open(files, 'rb'))
            print(files+' uploaded')
            
        else:
            raise
    else:
        exists = True

    print(files+''+' exists')
        


# In[ ]:




# In[ ]:




# In[ ]:



