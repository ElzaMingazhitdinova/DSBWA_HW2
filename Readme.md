## Формат запуска приложения

# Требования:
git
maven
docker
jdk 8
# Установка maven
    wget https://www-eu.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
    tar xzf apache-maven-3.6.3-bin.tar.gz
    ln -s apache-maven-3.6.3 maven
    vi /etc/profile.d/maven.sh
    С содержимым
    export M2_HOME=/opt/maven
    export PATH=${M2_HOME}/bin:${PATH}

#Запуск самого приложения:
./startHadoopContainer.sh hadoop-psql
он запускает start.sh (который запускается внутри docker контейнера)



