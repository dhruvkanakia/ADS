FROM python

COPY Ingestion.py /
COPY intial_config.json  /
COPY run.sh /
COPY properties_2016.zip /
COPY train_2016.zip /
RUN pip install requests



RUN pip install pandas





RUN pip install boto3

RUN pip install botocore


ADD run.sh /
RUN chmod +x /run.sh
ENTRYPOINT ["python","./Ingestion.py"]