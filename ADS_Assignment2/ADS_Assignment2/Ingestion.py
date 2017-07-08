
# coding: utf-8

# In[7]:

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
import zipfile


# In[8]:

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




# In[ ]:




# In[9]:

with zipfile.ZipFile('properties_2016.zip') as zip:
    with zip.open('properties_2016.csv') as myZip:
        a = pd.read_csv(myZip) 


# In[ ]:




# In[10]:

# a= pd.read_csv('properties_2016.csv')


# In[11]:

a.shape


# In[12]:

with zipfile.ZipFile('train_2016.zip') as zip:
    with zip.open('train_2016.csv') as myZip:
        b = pd.read_csv(myZip) 


# In[ ]:




# In[13]:

b.shape


# In[14]:

df= pd.merge(b,a, on='parcelid', how='left')


# In[15]:

df.head(2)


# In[16]:

df.shape


# In[17]:

df['transactiondate']=pd.to_datetime(df['transactiondate'])


# In[18]:

df['MONTH'] = pd.to_datetime(df['transactiondate']).dt.month
df['YEAR']  =   pd.to_datetime(df['transactiondate']).dt.year


# In[ ]:




# In[19]:

df.head()


# ## Looking at the correlation matrix

# In[20]:

correlation =df.corr()


# In[21]:

correlation.head()


# ## 1st Cleaning

# In[ ]:




# In[22]:

missing = df.isnull().sum(axis=0).reset_index()
missing.columns = ['name', 'count']
missing = missing.ix[missing['count']>0]
tot=df.shape[0]
missing['ratio']=missing['count'].apply(lambda x:x/tot)
missing = missing.sort_values(by='count')
missing.describe()


# ## Since 75% of the data is below 0.98 we removed all the columns that has ratio greater then 0.986901

# In[23]:

missing_columns_removing= missing[(missing['ratio']>0.986901)]
#bedcnt= bedcn[(bedcn['bedroomcnt']>0.5)]


# In[24]:

missing_columns_removing


# In[25]:

df.drop(['pooltypeid10','poolsizesum','decktypeid','finishedsquarefeet6','typeconstructiontypeid','architecturalstyletypeid','fireplaceflag','yardbuildingsqft26','storytypeid','basementsqft','finishedsquarefeet13','buildingclasstypeid'],axis=1, inplace=True)


# In[26]:

import numpy as np


# In[77]:

missing_df = pd.DataFrame({'Missing': df.isnull().sum()})
missing_df.sort_values(by="Missing", ascending=True, inplace=True)

a1 = np.arange(missing_df.shape[0])
fig,ax = plt.subplots(figsize=(10,15))
ax.barh(a1, missing_df['Missing'])
ax.set_yticks(a1)
ax.set_yticklabels(missing_df.index)
plt.show()


# ## 2nd cleaning

# In[28]:

pooltypeid2= pd.DataFrame(df['pooltypeid2']).reset_index()


# In[29]:

pooltypeid2.describe()


# ## Since all the values i.e the mean max is 1 in place of NaN we can put 1

# In[30]:

df['pooltypeid2']= df['pooltypeid2'].fillna(1)   ## Inserting 1 to the missing values


# In[31]:

df.shape


# ## 3rd Cleaning

# In[32]:

taxdelin_year= pd.DataFrame(df['taxdelinquencyyear'])


# In[33]:

get_ipython().magic('matplotlib inline')


# In[34]:

taxdelin_year.describe()


# In[35]:

taxdelin_year.taxdelinquencyyear.unique()


# In[36]:

taxdelin_year.shape


# ## One outlier with value 99. We removed that!

# In[37]:

taxdelin_year[(taxdelin_year['taxdelinquencyyear']==99.000000)]


# In[ ]:




# In[38]:

taxdelin_year=taxdelin_year.fillna(0)
taxdelin_year=pd.DataFrame(taxdelin_year.loc[(taxdelin_year['taxdelinquencyyear']<98.000000) ])


# In[39]:

taxdelin_year.taxdelinquencyyear.unique()

# taxdelin_year.describe()


# In[40]:

taxdelin_year_corr= pd.DataFrame(correlation['taxdelinquencyyear']).reset_index()
taxdelin_year_corr


# In[41]:

taxdelin_year = taxdelin_year.replace(0, np.NaN)


# In[42]:


mean_taxdelin_year= taxdelin_year['taxdelinquencyyear'].mean()
taxdelin_year['taxdelinquencyyear']=taxdelin_year.taxdelinquencyyear.mask(taxdelin_year.taxdelinquencyyear.isnull(), mean_taxdelin_year)
taxdelin_year.taxdelinquencyyear.describe()


# ### Changing the above values onto the main dataframe
# 

# In[43]:

df['taxdelinquencyyear']=pd.DataFrame(df['taxdelinquencyyear']).fillna(0)


# In[44]:

df.head()


# In[45]:

df=(df.loc[(df['taxdelinquencyyear']<98.000000) ])



# In[46]:

df['taxdelinquencyyear'] = pd.DataFrame(df['taxdelinquencyyear'].replace(0, np.NaN))


mean_taxdelin_year= df['taxdelinquencyyear'].mean(skipna=True)

df['taxdelinquencyyear'] =pd.DataFrame(df.taxdelinquencyyear.mask(df.taxdelinquencyyear.isnull(), mean_taxdelin_year))


# In[47]:


df.shape


# ### Cleaning 4

# In[48]:

taxdelin_flag= pd.DataFrame(df['taxdelinquencyflag'])


# In[49]:

taxdelin_flag.taxdelinquencyflag.unique()


# In[50]:

taxdelin_flag.describe()


# ### Since we can't figure out its correlation and it only has one unique value we can just put in 0

# In[51]:

df['taxdelinquencyflag']=pd.DataFrame(df['taxdelinquencyflag']).fillna(0)


# In[ ]:




# In[52]:

df.shape


# ## 5th Cleaning

# In[53]:

hashtutto= pd.DataFrame(df['hashottuborspa'])


# In[54]:

hashtutto.hashottuborspa.unique()


# In[55]:

hashtutto.describe()


# In[56]:

df['hashottuborspa']=pd.DataFrame(df['hashottuborspa']).fillna(0)


# In[57]:

df.shape


# ## 6th Cleaning

# In[58]:

yardbuilding= pd.DataFrame(df['yardbuildingsqft17'])


# In[59]:

yardbuilding.describe()


# In[60]:

yardbuilding_corr= pd.DataFrame(correlation['yardbuildingsqft17']).reset_index()
yardbuilding_corr[(yardbuilding_corr['yardbuildingsqft17']>0.3)]


# In[61]:

yardbuilding.plot(kind='box',ylim=(0,1000))


# ## inserting Mean values 

# In[62]:

df['yardbuildingsqft17'] = pd.DataFrame(df['yardbuildingsqft17'].replace(0, np.NaN))


mean_yard17= df['yardbuildingsqft17'].mean(skipna=True)

df['yardbuildingsqft17'] =pd.DataFrame(df.yardbuildingsqft17.mask(df.yardbuildingsqft17.isnull(), mean_yard17))


# In[ ]:




# ## 7th Cleaning
# 

# In[63]:

fullbathcnt= pd.DataFrame(df['fullbathcnt'])
fullbathcnt.fullbathcnt.unique()


# In[64]:

fullbathcnt.describe()


# In[65]:

fullbathcnt_corr= pd.DataFrame(correlation['fullbathcnt']).reset_index()
fullbathcnt_corr[(fullbathcnt_corr['fullbathcnt']>0.7)]


# In[66]:

df['mean_fullbathroomcnt']= (df['bathroomcnt']+df['calculatedbathnbr'])/2.000000


# In[67]:

abc= df[['mean_fullbathroomcnt','fullbathcnt']]


# In[68]:

abc.plot(kind='box',ylim=(0,7),figsize=(12,10))


# ## So, We will drop the full bath count column and use mean_fullbathroomcnt in place of it

# In[69]:


df.drop(['fullbathcnt'],axis=1, inplace=True)


# In[ ]:




# ## Inserting 0 in missing values of latittude and Longitudes

# In[ ]:




# In[70]:

df['latitude']=pd.DataFrame(df['latitude']).fillna(0)
df['longitude']=pd.DataFrame(df['longitude']).fillna(0)


# In[71]:

df.to_csv('Clean_Data.csv')


# In[ ]:




# In[72]:

logger.info('Reading Json')
with open('intial_config.json') as data_file:    
    data = json.load(data_file)


# In[73]:

import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])

client= boto3.client('s3', 
                     aws_access_key_id=data["AWSAccess"],
                    aws_secret_access_key=data["AWSSecret"])


# In[74]:

names=[]
response = client.list_buckets()
for bucket in response["Buckets"]:
    names.append(bucket)

Bucketname= 'adsassign2_databucket1'
if Bucketname in names:
    print('it exists')
else:
    s3.create_bucket(Bucket=Bucketname)  


# In[75]:

uploadFileNames = []

for filename in glob.glob("Clean_Data.csv"):
    uploadFileNames.append(filename)
    print(filename)


# In[76]:

import botocore.session
for files in uploadFileNames:
    try:
        s3.Object('adsassign2_databucket1', files).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            s3.Object("adsassign2_databucket1", files).put(Body=open(files, 'rb'))
            print(files+' uploaded')
            break
        else:
            raise
    else:
        exists = True

    print(files+''+' exists')
        


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:



