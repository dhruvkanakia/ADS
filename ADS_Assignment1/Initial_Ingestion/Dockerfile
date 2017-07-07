FROM python

COPY Intitial_Ingestion.py /
COPY intial_config.json  /
COPY run.sh /

RUN pip install requests



RUN pip install pandas





RUN pip install boto3

RUN pip install botocore


ADD run.sh /
RUN chmod +x /run.sh
ENTRYPOINT ["python","./Intitial_Ingestion.py"]