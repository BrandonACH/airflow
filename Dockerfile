FROM apache/airflow:2.6.0

#ENV MONGODB_USERNAME='brandonalfaro00'
#ENV MONGODB_PASSWORD='***'

RUN pip install pymongo apache-airflow-providers-mongo pandas requests openpyxl