FROM zoltannz/hadoop-ubuntu:2.8.1

RUN apt-get update -y
RUN apt-get install -y postgresql postgresql-contrib

RUN wget http://apache-mirror.rbc.ru/pub/apache/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz -O /download_sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
RUN wget --no-check-certificate https://jdbc.postgresql.org/download/postgresql-42.2.5.jar -O /download_postgresql-42.2.5.jar
RUN wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz -O /download_spark-2.3.1-bin-hadoop2.7.tgz

