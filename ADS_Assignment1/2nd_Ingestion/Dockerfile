FROM python

COPY 2nd_Ingestion.py /
COPY config.json  /
COPY run.sh /
COPY RawEDA.ipynb /
RUN pip install requests



RUN pip install pandas





RUN pip install boto3

RUN pip install botocore


ADD run.sh /
RUN chmod +x /run.sh
ENTRYPOINT ["python","./2nd_Ingestion.py"]