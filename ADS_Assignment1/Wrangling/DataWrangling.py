
# coding: utf-8

# In[11]:

import matplotlib.pyplot as plt
import datetime as dt
import numpy as np
import pandas as pd
import datetime as dt
import numpy as np
import pandas as pd
import json
import glob


# In[12]:

with open('wrangle_config.json') as data_file:    
    data = json.load(data_file)



# In[13]:

import time
Date= time.strftime('%Y-%m-%d')


# In[14]:

file=data['state']+'_'+ Date+'_'+ '23155_'+ 'clean' + '.csv'


# In[15]:

file


# In[18]:

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
s3.Bucket('adsassign1_databucket').download_file(key_to_download, key_to_download)
df=pd.read_csv(key_to_download)
# obj = client.get_object(Bucket='adsassign1_databucket', Key=a)
# df = pd.read_csv(key_to_download)


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[19]:

import pandas as pd
df['DATE']=pd.to_datetime(df['DATE'])


# In[10]:

import datetime as dt
df['MONTH'] = pd.to_datetime(df['DATE']).dt.month
df['YEAR']  =   pd.to_datetime(df['DATE']).dt.year
# df[['MONTH','YEAR']]




# In[20]:

df.head()


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




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[21]:

df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].str.replace('s','')
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].str.replace('V','')
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].fillna(0)
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].astype(float)
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].astype(float)
mean_station_pressure =df["HOURLYVISIBILITY"].mean(skipna=True)
df["HOURLYVISIBILITY"]=df.HOURLYVISIBILITY.mask(df.HOURLYVISIBILITY == 0,mean_station_pressure)




df["HOURLYStationPressure"]=df["HOURLYStationPressure"].str.replace('s','')
df["HOURLYStationPressure"]=df["HOURLYStationPressure"].str.replace('V','')
df["HOURLYStationPressure"]=df["HOURLYStationPressure"].fillna(0)
df["HOURLYStationPressure"]=df["HOURLYStationPressure"].astype(float)
df["HOURLYStationPressure"]=df["HOURLYStationPressure"].astype(float)
mean_station_pressure =df["HOURLYStationPressure"].mean(skipna=True)
df["HOURLYStationPressure"]=df.HOURLYStationPressure.mask(df.HOURLYStationPressure == 0,mean_station_pressure)

df["DAILYPeakWindSpeed"]=df["DAILYPeakWindSpeed"].str.replace('s','')
df["DAILYPeakWindSpeed"]=df["DAILYPeakWindSpeed"].str.replace('V','')
df["DAILYPeakWindSpeed"]=df["DAILYPeakWindSpeed"].fillna(0)
df["DAILYPeakWindSpeed"]=df["DAILYPeakWindSpeed"].astype(float)
df["DAILYPeakWindSpeed"]=df["DAILYPeakWindSpeed"].astype(float)
mean_station_pressure =df["DAILYPeakWindSpeed"].mean(skipna=True)
df["DAILYPeakWindSpeed"]=df.DAILYPeakWindSpeed.mask(df.DAILYPeakWindSpeed == 0,mean_station_pressure)


df["HOURLYDewPointTempC"]=df["HOURLYDewPointTempC"].str.replace('s','')
df["HOURLYDewPointTempC"]=df["HOURLYDewPointTempC"].str.replace('V','')
df["HOURLYDewPointTempC"]=df["HOURLYDewPointTempC"].fillna(0)
df["HOURLYDewPointTempC"]=df["HOURLYDewPointTempC"].astype(float)
df["HOURLYDewPointTempC"]=df["HOURLYDewPointTempC"].astype(float)
mean_station_pressure =df["HOURLYDewPointTempC"].mean(skipna=True)
df["HOURLYDewPointTempC"]=df.HOURLYDewPointTempC.mask(df.HOURLYDewPointTempC == 0,mean_station_pressure)



df["MonthlyTotalHeatingDegreeDays"]=df["MonthlyTotalHeatingDegreeDays"].str.replace('s','')
df["MonthlyTotalHeatingDegreeDays"]=df["MonthlyTotalHeatingDegreeDays"].str.replace('V','')
df["MonthlyTotalHeatingDegreeDays"]=df["MonthlyTotalHeatingDegreeDays"].fillna(0)
df["MonthlyTotalHeatingDegreeDays"]=df["MonthlyTotalHeatingDegreeDays"].astype(float)
df["MonthlyTotalHeatingDegreeDays"]=df["MonthlyTotalHeatingDegreeDays"].astype(float)
mean_station_pressure =df["MonthlyTotalHeatingDegreeDays"].mean(skipna=True)
df["MonthlyTotalHeatingDegreeDays"]=df.MonthlyTotalHeatingDegreeDays.mask(df.MonthlyTotalHeatingDegreeDays == 0,mean_station_pressure)


df["MonthlyTotalCoolingDegreeDays"]=df["MonthlyTotalCoolingDegreeDays"].str.replace('s','')
df["MonthlyTotalCoolingDegreeDays"]=df["MonthlyTotalCoolingDegreeDays"].str.replace('V','')
df["MonthlyTotalCoolingDegreeDays"]=df["MonthlyTotalCoolingDegreeDays"].fillna(0)
df["MonthlyTotalCoolingDegreeDays"]=df["MonthlyTotalCoolingDegreeDays"].astype(float)
df["MonthlyTotalCoolingDegreeDays"]=df["MonthlyTotalCoolingDegreeDays"].astype(float)
mean_station_pressure =df["MonthlyTotalCoolingDegreeDays"].mean(skipna=True)
df["MonthlyTotalCoolingDegreeDays"]=df.MonthlyTotalCoolingDegreeDays.mask(df.MonthlyTotalCoolingDegreeDays == 0,mean_station_pressure)






# In[22]:

df1= df[['DATE','DAILYAverageDewPointTemp','DAILYAverageDryBulbTemp','DAILYAverageRelativeHumidity','DAILYAverageSeaLevelPressure',
         'DAILYAverageStationPressure','DAILYAverageWetBulbTemp','DAILYAverageWindSpeed','DAILYCoolingDegreeDays','DAILYDeptFromNormalAverageTemp','DAILYHeatingDegreeDays',
         'DAILYMaximumDryBulbTemp','DAILYMinimumDryBulbTemp',
'DAILYPeakWindSpeed',

'HOURLYAltimeterSetting','HOURLYDRYBULBTEMPC','HOURLYDRYBULBTEMPF','HOURLYDewPointTempC','HOURLYDewPointTempF',
'HOURLYPrecip','HOURLYPressureChange','HOURLYPressureTendency','HOURLYRelativeHumidity',
'HOURLYSeaLevelPressure','HOURLYStationPressure','HOURLYVISIBILITY','HOURLYWETBULBTEMPC','HOURLYWETBULBTEMPF','HOURLYWindDirection','MonthlyMaximumTemp','MonthlyMinimumTemp',
        'MonthlyMeanTemp','MonthlyStationPressure','MonthlySeaLevelPressure','MonthlyDeptFromNormalMaximumTemp','MonthlyDeptFromNormalMinimumTemp',
        'MonthlyDeptFromNormalAverageTemp','MonthlyDeptFromNormalPrecip','MonthlyDeptFromNormalPrecip','MonthlyTotalLiquidPrecip',
        'MonthlyDaysWithGT90Temp','MonthlyDaysWithGT32Temp','MonthlyTotalHeatingDegreeDays','MonthlyTotalCoolingDegreeDays','MonthlyDeptFromNormalHeatingDD',
        'MonthlyDeptFromNormalCoolingDD','MonthlyTotalSeasonToDateCoolingDD']]


# In[23]:

df1.to_csv(file)


# In[24]:

import boto3
s3 = boto3.resource(
    's3',
    aws_access_key_id=data["AWSAccess"],
    aws_secret_access_key=data["AWSSecret"])


# In[25]:

uploadFileNames = []

for filename in glob.glob("*.csv"):
    uploadFileNames.append(filename)
    print(filename)


# In[26]:

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



