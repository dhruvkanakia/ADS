

# Assignment 1


Hi, our name is Dhruv Kanakia and Akil Ranjendiran and we belong to TEAM 5.

In this Assignment we have worked on NOAA dataset (https://www.ncdc.noaa.gov/cdo-web/datatools/lcd). The state in which we have done our analysis is California. 

## STEPS to follow while running the code:
a. INITIAL INGESTION 
b. 2nd Ingestion
c. Wrangling

## The workflow of the process being:

### Initial Ingestion: [https://github.com/dhruvkanakia/ADS_Summer/blob/master/ADS_Assignment1/Initial_Ingestion/Intitial_Ingestion.ipynb](url)
1. Downloading 46 years of data from the website(https://www.ncdc.noaa.gov/cdo-web/datatools/lcd) and storing it on s3 as well as local system. 
Below is the code we have used to concatenate the 46 years data into a csv file

import io
df_list= []
for x in links:
    
s=requests.get(x).content
c=pd.read_csv(io.StringIO(s.decode('utf-8')))
df_list.append(c)
full_df=pd.concat(df_list)




2.Below is the code used to add the created csv file into amazon s3


`import botocore.session
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

    print(files+''+' exists')`

## 2nd Ingestion
[https://github.com/dhruvkanakia/ADS_Summer/blob/master/ADS_Assignment1/2nd_Ingestion/2nd_Ingestion.ipynb](url)

1. In this file we concatenate the new data that is getting downloaded from the link(in the json) with the previously mentioned csv file.

Below is the code used to concatenate the new files with the old ones.

`b= list(bucket.objects.all())
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
    full_df.to_csv(new_file)`


2. The csv file created is transferred to the s3 with the date it is run at also checking if it is previously present or not.

3. The csv file which was transferred on to amazon is initialized in a dataframe to do Data Exploration.

### Below are the few images attached of the Data Exploration on the most recently uploaded raw file.


The plot shows visibility over the years

1. ![download](https://user-images.githubusercontent.com/10628795/27513475-5db5705a-5935-11e7-9038-619a469a399d.png)

This plot shows heating days over the years

2. ![download 1](https://user-images.githubusercontent.com/10628795/27513484-7e9914d4-5935-11e7-81c7-c8313237a06f.png)

The data was not that clean to perform good analysis. SO to clean the data from the amazon server and do data wrangling on it
      
### DATA WRANGLING:
[https://github.com/dhruvkanakia/ADS_Summer/blob/master/ADS_Assignment1/Wrangling/DataWrangling.ipynb](url)

The unclean csv which was used for data analysis is now being initialized in this process to do data wrangling on it.

Below are few methods we used for data wrangling and fill up missing values.


This code removes the string values present in that column. THe problem we were facing while plotting a graph for it.

`df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].str.replace('s','')
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].str.replace('V','')
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].fillna(0)
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].astype(float)
df["HOURLYVISIBILITY"]=df["HOURLYVISIBILITY"].astype(float)
mean_station_pressure =df["HOURLYVISIBILITY"].mean(skipna=True)
df["HOURLYVISIBILITY"]=df.HOURLYVISIBILITY.mask(df.HOURLYVISIBILITY == 0,mean_station_pressure)`

The below code removes all the unwanted columns which had no significant contribution to the file.

`
df1= df[['DATE','DAILYAverageDewPointTemp','DAILYAverageDryBulbTemp','DAILYAverageRelativeHumidity','DAILYAverageSeaLevelPressure',
         'DAILYAverageStationPressure','DAILYAverageWetBulbTemp','DAILYAverageWindSpeed','DAILYCoolingDegreeDays','DAILYDeptFromNormalAverageTemp','DAILYHeatingDegreeDays',
         'DAILYMaximumDryBulbTemp','DAILYMinimumDryBulbTemp',
'DAILYPeakWindSpeed',
​
'HOURLYAltimeterSetting','HOURLYDRYBULBTEMPC','HOURLYDRYBULBTEMPF','HOURLYDewPointTempC','HOURLYDewPointTempF',
'HOURLYPrecip','HOURLYPressureChange','HOURLYPressureTendency','HOURLYRelativeHumidity',
'HOURLYSeaLevelPressure','HOURLYStationPressure','HOURLYVISIBILITY','HOURLYWETBULBTEMPC','HOURLYWETBULBTEMPF','HOURLYWindDirection','MonthlyMaximumTemp','MonthlyMinimumTemp',
        'MonthlyMeanTemp','MonthlyStationPressure','MonthlySeaLevelPressure','MonthlyDeptFromNormalMaximumTemp','MonthlyDeptFromNormalMinimumTemp',
        'MonthlyDeptFromNormalAverageTemp','MonthlyDeptFromNormalPrecip','MonthlyDeptFromNormalPrecip','MonthlyTotalLiquidPrecip',
        'MonthlyDaysWithGT90Temp','MonthlyDaysWithGT32Temp','MonthlyTotalHeatingDegreeDays','MonthlyTotalCoolingDegreeDays','MonthlyDeptFromNormalHeatingDD',
        'MonthlyDeptFromNormalCoolingDD','MonthlyTotalSeasonToDateCoolingDD']]`


### After the data is cleaned we uploaded the clean csv back to the amazon 
[https://github.com/dhruvkanakia/ADS_Summer/blob/master/ADS_Assignment1/Wrangling/EDA_Wrangling.ipynb](url)


4. The next part uses the clean file to do exploratory analysis on the clean file.

1. While plotting graph for Temperature in california along the years we came across that it is highest in 2014. The below graph shows that:

![download 3](https://user-images.githubusercontent.com/10628795/27513513-c2518bce-5936-11e7-809c-217c341265b0.png)


## This also proved that 2014 was the year when the temperature reached its peak.

2. To get more into this temperature issue we tried to figure out how was the distribution of temperature in terms of months and compared it with average monthly temperature over the years.

The nelow graph shows that:

![download 2](https://user-images.githubusercontent.com/10628795/27513517-022b672e-5937-11e7-845b-d2678c89904b.png)


3. We also mamaged to plot graphs for some column after removing the string values. One of the example graph is showed below:


![download 4](https://user-images.githubusercontent.com/10628795/27513523-1d9f3490-5937-11e7-9f18-e48b59e5ff3b.png)


All the graphs can be seen in the EDA_Wrangling file.
