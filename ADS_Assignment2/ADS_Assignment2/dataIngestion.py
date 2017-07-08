
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
import plotly as py


# In[112]:

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


# In[113]:

logger.info('Reading Json')
with open('intial_config.json') as data_file:    
    data = json.load(data_file)


# In[114]:

import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])

client= boto3.client('s3', 
                     aws_access_key_id=data["AWSAccess"],
                    aws_secret_access_key=data["AWSSecret"])


# In[115]:

names=[]
response = client.list_buckets()
for bucket in response["Buckets"]:
    names.append(bucket)

Bucketname= 'adsassign2_databucket1'
if Bucketname in names:
    print('it exists')
else:
    s3.create_bucket(Bucket=Bucketname)  


# In[123]:

uploadFileNames = []

for filename in glob.glob("Clean_Data.csv"):
    uploadFileNames.append(filename)
    print(filename)


# In[124]:

import botocore.session
for files in uploadFileNames:
    try:
        s3.Object('adsassign2_databucket1', files).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            s3.Object("adsassign1_databucket", files).put(Body=open(files, 'rb'))
            print(files+' uploaded')
            
        else:
            raise
    else:
        exists = True

    print(files+''+' exists')
        


# In[2]:

a= pd.read_csv('properties_2016.csv')


# In[3]:

a.shape


# In[4]:

b= pd.read_csv('train_2016.csv')


# In[5]:

b.shape


# In[95]:

df= pd.merge(b,a, on='parcelid', how='left')


# In[96]:

df.head()


# In[8]:

df.shape


# In[9]:

df['transactiondate']=pd.to_datetime(df['transactiondate'])


# In[10]:

df['MONTH'] = pd.to_datetime(df['transactiondate']).dt.month
df['YEAR']  =   pd.to_datetime(df['transactiondate']).dt.year


# In[ ]:




# In[11]:

df.head()


# ### Single Variable Analysis

# In[12]:

monthly= df.groupby(['MONTH'])['YEAR'].count()


# In[13]:

monthly1= pd.DataFrame(monthly).reset_index()


# In[14]:

import calendar
monthly1['MONTH'] = monthly1['MONTH'].apply(lambda x: calendar.month_abbr[x])


# In[91]:

from scipy import stats, integrate
import matplotlib.pyplot as plt
import seaborn as sns


# In[22]:

import plotly.plotly as py
import plotly.graph_objs as go
import plotly.figure_factory as FF


# In[83]:

import plotly 
plotly.tools.set_credentials_file(username='dhruvkanakia', api_key='qHQ6b8DwjCTt3UUxTxfc')


# In[24]:


trace = go.Scatter(x = monthly1['MONTH'], y = monthly1['YEAR'],
                  name='Number of Transactions')
layout = go.Layout(title='Number of Transactions over each month',
                   plot_bgcolor='rgb(180, 180,180)', 
                   showlegend=True)
fig = go.Figure(data=[trace], layout=layout)

py.iplot(fig, filename='apple-stock-prices')


# ### So we can clearly see that there are highest number of transactinos between May and September with highest being in JUNE. Chances are that this is the period when move in and move out's normally take place.

# In[27]:

most_number_built= df[['yearbuilt','roomcnt']]


# In[28]:

most_number_built= most_number_built.groupby('yearbuilt').count().reset_index()


# In[35]:

get_ipython().magic('matplotlib inline')
most_number_built.plot(kind='scatter',x='yearbuilt',y='roomcnt',xlim=(1890,2017),ylim=(0,2500))


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[58]:

missing = df.isnull().sum(axis=0).reset_index()
missing.columns = ['name', 'count']
missing = missing.ix[missing['count']>0]
tot=df.shape[0]
missing['ratio']=missing['count'].apply(lambda x:x/tot)
missing = missing.sort_values(by='count')
missing.describe()


# ## Since 75% of the data is below 0.98 we removed all the columns that has ratio greater then 0.986901

# In[81]:

missing_columns_removing= missing[(missing['ratio']>0.986901)]
#bedcnt= bedcn[(bedcn['bedroomcnt']>0.5)]


# In[82]:

missing_columns_removing


# In[97]:

df.drop(['pooltypeid10','poolsizesum','decktypeid','finishedsquarefeet6','typeconstructiontypeid','architecturalstyletypeid','fireplaceflag','yardbuildingsqft26','storytypeid','basementsqft','finishedsquarefeet13','buildingclasstypeid'],axis=1, inplace=True)


# In[ ]:




# In[100]:

missing_df = pd.DataFrame({'Missing': df.isnull().sum()})
missing_df.sort_values(by="Missing", ascending=True, inplace=True)

a1 = np.arange(missing_df.shape[0])
fig,ax = plt.subplots(figsize=(10,15))
ax.barh(a1, missing_df['Missing'])
ax.set_yticks(a1)
ax.set_yticklabels(missing_df.index)
plt.show()


# In[ ]:




# In[106]:

pooltypeid2= pd.DataFrame(df['pooltypeid2']).reset_index()


# In[109]:

pooltypeid2.describe()


# ## Since all the values i.e the mean max is 1 in place of NaN we can put 1

# In[110]:

df['pooltypeid2']= df['pooltypeid2'].fillna(1)


# In[ ]:




# In[111]:

df.to_csv('Clean_Data.csv')


# In[74]:

from pylab import *
import matplotlib.pyplot as plt
get_ipython().magic('matplotlib inline')


# In[75]:

#correlation matrix
corrmat = df.corr()
f, ax = plt.subplots(figsize=(18,12 ))
sns.heatmap(corrmat, vmax=.8, square=True);


# In[43]:

x=df.corr()


# In[55]:

bedcn= pd.DataFrame(x['bedroomcnt']).reset_index()


# In[60]:

bedcn.describe()


# In[62]:

bedcn.head(2)


# In[68]:

bedcnt= bedcn[(bedcn['bedroomcnt']>0.5)]


# In[69]:

bedcnt


# In[71]:

df.plot(kind='scatter',x='bedroomcnt',y='finishedsquarefeet12',figsize=(15,12))


# In[ ]:




# In[85]:

import plotly.plotly as py
import plotly.graph_objs as go

import numpy as np

x, y, z = df['finishedsquarefeet12']
trace1 = go.Scatter3d(
    x=x,
    y=y,
    z=z,
    mode='markers',
    marker=dict(
        size=12,
        line=dict(
            color='rgba(217, 217, 217, 0.14)',
            width=0.5
        ),
        opacity=0.8
    )
)

x2, y2, z2 = df['bathroomcnt']
trace2 = go.Scatter3d(
    x=x2,
    y=y2,
    z=z2,
    mode='markers',
    marker=dict(
        color='rgb(127, 127, 127)',
        size=12,
        symbol='circle',
        line=dict(
            color='rgb(204, 204, 204)',
            width=1
        ),
        opacity=0.9
    )
)
data = [trace1, trace2]
layout = go.Layout(
    margin=dict(
        l=0,
        r=0,
        b=0,
        t=0
    )
)
fig = go.Figure(data=data, layout=layout)
py.iplot(fig, filename='simple-3d-scatter')


# In[15]:

get_ipython().magic('matplotlib notebook')
get_ipython().magic('matplotlib notebook')


# In[17]:

from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt


fig = plt.figure(figsize=(10,5)).gca(projection='3d')
#ax = fig.add_subplot(111, projection='3d')
x =df['finishedsquarefeet12']
y =df['bedroomcnt']
z =df['bathroomcnt']



fig.scatter(x, y, z)

fig.set_xlabel('finishedsquarefeet12')
fig.set_ylabel('bedroomcnt')
fig.set_zlabel('bathroomcnt')


# In[50]:

df_bedroomcount=a[pd.isnull(a['bedroomcnt'])]# bedcnt= bedcn[(bedcn['bedroomcnt']>0.5)]


# In[53]:

df_bedroomcount


# In[ ]:




# In[ ]:




# In[ ]:




# In[44]:

import numpy as np


# In[45]:

missing_df = pd.DataFrame({'Missing': a.isnull().sum()})
missing_df.sort_values(by="Missing", ascending=True, inplace=True)

a1 = np.arange(missing_df.shape[0])
fig,ax = plt.subplots(figsize=(10,15))
ax.barh(a1, missing_df['Missing'])
ax.set_yticks(a1)
ax.set_yticklabels(missing_df.index)
plt.show()


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



