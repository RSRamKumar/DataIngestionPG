FROM python:latest

WORKDIR /ingestion_dir

COPY data_ingestion_script.py /ingestion_dir

RUN pip install pandas sqlalchemy  psycopg2-binary

ENTRYPOINT ["python", "data_ingestion_script.py"]