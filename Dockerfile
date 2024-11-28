FROM bde2020/spark-python-template:3.1.2-hadoop3.2

ENV SPARK_APPLICATION_PYTHON_LOCATION app/app1.py
ENV SPARK_APPLICATION_ARGS "task1 hdfs://namenode:9000/project/emissions.csv 11.552373908130049 48.1443852206526 1 0 100"
