#!/bin/bash

if [[ $# -eq 0 ]] ; then
    echo 'You should specify database name!'
    exit 1
fi

export PATH=$PATH:/usr/local/hadoop/bin/
export PATH=$PATH:/sqoop-1.4.7.bin__hadoop-2.6.0/bin
export SPARK_HOME=/spark-2.3.1-bin-hadoop2.7
export HADOOP_CONF_DIR=$HADOOP_PREFIX/etc/hadoop
export PATH=$PATH:/spark-2.3.1-bin-hadoop2.7/bin

# Устанавливаем PostgreSQL
sudo apt-get update -y
sudo apt-get install -y postgresql postgresql-contrib python3-pip
sudo service postgresql start

# Создаем таблицы
sudo -u postgres psql -c 'ALTER USER postgres PASSWORD '\''1234'\'';'
sudo -u postgres psql -c 'drop database if exists '"$1"';'
sudo -u postgres psql -c 'create database '"$1"';'
sudo -u postgres -H -- psql -d $1 -c 'CREATE TABLE arraycompute (id BIGSERIAL PRIMARY KEY, key int, value int, other VARCHAR(256));'
sudo -u postgres -H -- psql -d $1 -c 'CREATE TABLE arraydata (id BIGSERIAL PRIMARY KEY, index int, key int, value int, other VARCHAR(256));'

# Скачиваем SQOOP
if [ ! -f sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz ]; then
    wget http://apache-mirror.rbc.ru/pub/apache/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
    tar xvzf sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz
else
    echo "Sqoop already exists, skipping..."
fi

# Скачиваем драйвер PostgreSQL
if [ ! -f postgresql-42.2.5.jar ]; then
    wget --no-check-certificate https://jdbc.postgresql.org/download/postgresql-42.2.5.jar
    cp postgresql-42.2.5.jar sqoop-1.4.7.bin__hadoop-2.6.0/lib/
else
    echo "Postgresql driver already exists, skipping..."
fi

# Скачиваем Spark
if [ ! -f spark-2.3.1-bin-hadoop2.7.tgz ]; then
    wget https://archive.apache.org/dist/spark/spark-2.3.1/spark-2.3.1-bin-hadoop2.7.tgz
    tar xvzf spark-2.3.1-bin-hadoop2.7.tgz
else
    echo "Spark already exists, skipping..."
fi

# Создаем исходные маcсивы длины 1 с константными значениями
# Для computeIntensive
sudo -u postgres -H -- psql -d $1 -c 'INSERT INTO arraycompute (key, value, other) values (1,20,'\'test\'');'
# Для dataIntensive создадим 100 массивов
for i in {1..100}
do
    sudo -u postgres -H -- psql -d $1 -c 'INSERT INTO arraydata (index, key, value, other) values ('$i',1,1000,'\'test\'');'
done

# Перебираем размер маcсива = 2^i
for i in {0..24}
do
    key_shift=$(python3 -c "print(2**$i)")
    N=$(python3 -c "print(2**($i + 1))")

    # Запускаем computeIntensive эксперимент

    # Увеличиваем длину маcсива в 2 раза
    sudo -u postgres -H -- psql -d $1 -c 'INSERT INTO arraycompute (key, value, other) SELECT key + '$key_shift', value, other FROM arraycompute;'

    sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"$1"'?ssl=false' --username 'postgres' --password '1234' --table 'arraycompute' --target-dir 'arraycompute'

    { time spark-submit --class bdtc.lab2.SparkSQLApplication --master local --deploy-mode client --executor-memory 1g --name arraycompute --conf "spark.app.id=SparkSQLApplication" /tmp/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://127.0.0.1:9000/user/root/arraycompute/ out computeIntensive ; } 2> /execution_time_one_iter.log
    echo $N >> /execution_time_computeIntensive.log
    cat /execution_time_one_iter.log >> /execution_time_computeIntensive.log

    echo "DONE! RESULT IS: "
    hadoop fs -tail  hdfs://127.0.0.1:9000/user/root/out/part-00000

    # Освобождаем память
    hadoop dfs -rm -r arraycompute
    hadoop dfs -rm -r out


    # Запускаем dataIntensive эксперимент

    # Увеличиваем длину масива в 2 раза
    sudo -u postgres -H -- psql -d $1 -c 'INSERT INTO arraydata (index, key, value, other) SELECT index, key + '$key_shift', value, other FROM arraydata;'

    sqoop import --connect 'jdbc:postgresql://127.0.0.1:5432/'"$1"'?ssl=false' --username 'postgres' --password '1234' --table 'arraydata' --target-dir 'arraydata'

    { time spark-submit --class bdtc.lab2.SparkSQLApplication --master local --deploy-mode client --executor-memory 1g --name arraydata --conf "spark.app.id=SparkSQLApplication" /tmp/lab2-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://127.0.0.1:9000/user/root/arraydata/ out dataIntensive ; } 2> /execution_time_one_iter.log
    echo $N >> /execution_time_dataIntensive.log
    cat /execution_time_one_iter.log >> /execution_time_dataIntensive.log

    echo "DONE! RESULT IS: "
    hadoop fs -tail  hdfs://127.0.0.1:9000/user/root/out/part-00000

    # Освобождаем память
    hadoop dfs -rm -r arraydata
    hadoop dfs -rm -r out

done
