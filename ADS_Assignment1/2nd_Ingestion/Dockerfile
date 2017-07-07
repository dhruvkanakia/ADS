FROM python

COPY 2nd_Ingestion.py /
COPY config.json  /

RUN pip install requests



RUN pip install pandas





RUN pip install boto3

RUN pip install botocore



CMD ["python","2nd_Ingestion.py"]