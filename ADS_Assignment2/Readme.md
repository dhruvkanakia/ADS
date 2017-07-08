

```python
# Assignment 1


Hi, our name is Dhruv Kanakia and Akil Ranjendiran and we belong to TEAM 5.

In this Assignment we have worked on NOAA dataset (https://www.ncdc.noaa.gov/cdo-web/datatools/lcd). The state in which we have done our analysis is California. 

## STEPS to follow while running the code:
1.Ingestion.py runs the code that puts the data onto amazon
2. RawEda.pynb is used to plot the data


1. Downloading the zillow dataset from kaggle.com and passing it onto ingestionpy to clean it and upload the clean data on amazon.

Below are the commands to run the image

command docker pull dhruvkanakia/adv_data_science:ingestion
        docker run  dhruvkanakia/adv_data_science:ingestion
            
            
2. RawEDA shows plots that have been created using the raw data. Few of the example plots have been shown below.

The following image shows the number of transactions done using the train dataset. 

![yearbuild](https://user-images.githubusercontent.com/29669364/27982285-b0db09fc-636c-11e7-91c0-d18262a204e1.png)


It clearly shows that the in between months from June to DEcember it had the highest transaction. This can indicate maybe it is the time of move outs and move ins


The second plot shows the relation between three attributes: bedroom count, bathroom count and square. 
    
![untitled](https://user-images.githubusercontent.com/29669364/27982297-fa87da94-636c-11e7-85bc-ece2dd06d732.png)



After getting the clean files we are uploading the dataset on IBM CLOUDANT using couchimport

Following screenshot shows that it is writing onto cloudant without erros

<img width="960" alt="untitled" src="https://user-images.githubusercontent.com/29669364/27982396-93372e38-636e-11e7-9967-b49d29bed456.png">

The following image shows that data has been uploaded

<img width="735" alt="untitled" src="https://user-images.githubusercontent.com/29669364/27982412-eb1b93e6-636e-11e7-8d1a-431cca2ad3f3.png">

THE following two images shows the UI to input lattitude and longitude while the next image shows the data being extracted from the cloudant dataset.

![1](https://user-images.githubusercontent.com/29669364/27982436-5933c3d0-636f-11e7-93d4-889ddbd5c93f.png)


![2](https://user-images.githubusercontent.com/29669364/27982448-7bb25c82-636f-11e7-99ee-501fd97415ad.PNG)
```
