FROM python

COPY DataWrangling.py /
COPY wrangle_config.json  /

RUN pip install requests



RUN pip install pandas


RUN pip install matplotlib


RUN pip install boto3

RUN pip install botocore



CMD ["python","DataWrangling.py"]