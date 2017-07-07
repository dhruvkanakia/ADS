FROM python

COPY DataWrangling.py /
COPY wrangle_config.json  /
COPY run.sh /
COPY DataWrangling.ipynb /
RUN pip install requests



RUN pip install pandas


RUN pip install matplotlib


RUN pip install boto3

RUN pip install botocore


ADD run.sh /
RUN chmod +x /run.sh
ENTRYPOINT ["python","./DataWrangling.py"]
