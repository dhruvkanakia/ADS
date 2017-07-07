FROM python

COPY Intitial_Ingestion.py /
COPY intial_config.json  /

RUN pip install requests



RUN pip install pandas





RUN pip install boto3

RUN pip install botocore



CMD ["python","Intitial_Ingestion.py"]