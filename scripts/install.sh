#!/bin/bash
apt-get update
apt-get upgrade -y
apt-get install python-setuptools python-dev build-essential python-pip -y
pip install numpy sklearn ipython scipy python-geoip python-geoip-geolite2
apt-get install openssh-server openssh-client -y
sed -i "1s/.*/127.0.0.1       localhost       master/" /etc/hosts
ssh-keygen -t rsa -P "" -f ~/.ssh/id_rsa
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
service ssh restart
cd ~
apt install openjdk-8-jdk openjdk-8-jre -y
wget http://ftp.unicamp.br/pub/apache/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz
mkdir -p /opt/hadoop
tar -xzvf hadoop-3.2.1.tar.gz -C /opt/hadoop/ --strip-components=1
rm hadoop-3.2.1.tar.gz
echo $'# -- HADOOP ENVIRONMENT VARIABLES START -- #
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib/native"
# -- HADOOP ENVIRONMENT VARIABLES END -- #' >> ~/.bashrc
. ~/.bashrc
echo $'JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export YARN_RESOURCEMANAGER_USER=root
export YARN_NODEMANAGER_USER=root
export HDFS_NAMENODE_USER=root
export HDFS_SECONDARYNAMENODE_USER=root
export HDFS_DATANODE_USER=root
export HDFS_JOURNALNODE_USER=root' >> /opt/hadoop/etc/hadoop/hadoop-env.sh
echo $'<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
    <name>fs.default.name</name>
    <value>hdfs://master:9000</value>
</property>

</configuration>' > /opt/hadoop/etc/hadoop/core-site.xml
echo $'<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
      <name>dfs.replication</name>
      <value>1</value>
</property>
<property>
      <name>dfs.namenode.name.dir</name>
      <value>file:/opt/hadoop_tmp/hdfs/namenode</value>
</property>
<property>
      <name>dfs.datanode.data.dir</name>
      <value>file:/opt/hadoop_tmp/hdfs/datanode</value>
</property>

</configuration>' > /opt/hadoop/etc/hadoop/hdfs-site.xml
echo $'<?xml version="1.0"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
<configuration>

<!-- Site specific YARN configuration properties -->
<property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>master:8025</value>
</property>
<property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>master:8035</value>
</property>
<property>
    <name>yarn.resourcemanager.address</name>
    <value>master:8050</value>
</property>

</configuration>' > /opt/hadoop/etc/hadoop/yarn-site.xml
echo $'<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

<!-- Put site-specific property overrides in this file. -->

<configuration>

<property>
    <name>mapreduce.job.tracker</name>
    <value>master:5431</value>
</property>
<property>
    <name>mapred.framework.name</name>
    <value>yarn</value>
</property>

</configuration>' > /opt/hadoop/etc/hadoop/mapred-site.xml
mkdir -p /opt/hadoop_tmp/hdfs/namenode
mkdir -p /opt/hadoop_tmp/hdfs/datanode
echo $'localhost' >> /opt/hadoop/etc/hadoop/masters
echo $'localhost' >> /opt/hadoop/etc/hadoop/slaves
. ~/.bashrc
hdfs namenode -format
/opt/hadoop/sbin/start-dfs.sh
/opt/hadoop/sbin/start-yarn.sh
hdfs dfsadmin -safemode leave
hadoop fs -mkdir /user
hadoop fs -mkdir /user/app
hadoop fs -mkdir /user/app/kafkaCheckpoint
hadoop fs -mkdir /user/app/elasticCheckpoint
apt-get install scala -y
wget https://downloads.apache.org/spark/spark-2.4.7/spark-2.4.7-bin-hadoop2.7.tgz
mkdir /opt/spark
tar xzf spark-2.4.7-bin-hadoop2.7.tgz -C /opt/spark --strip-components=1
rm spark-2.4.7-bin-hadoop2.7.tgz
echo $'export SPARK_HOME=/opt/spark
export PATH=$PATH:$SPARK_HOME/bin
export PYSPARK_DRIVER_PYTHON=ipython' >> ~/.bashrc
. ~/.bashrc
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
echo $'export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export SPARK_WORKER_CORES=8
SPARK_MASTER_HOST=localhost' >> $SPARK_HOME/conf/spark-env.sh
echo $'localhost' > $SPARK_HOME/conf/slaves
mv /opt/spark/conf/spark-defaults.conf.template /opt/spark/conf/spark-defaults.conf
echo $'spark.master                     spark://master:7077
spark.eventLog.enabled           true
spark.eventLog.dir               /tmp/
spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory              5g
spark.executor.extraJavaOptions  -XX:+PrintGCDetails -Dkey=value -Dnumbers="on$' >> /opt/spark/conf/spark-defaults.conf
mkdir /tmp/spark-events
apt-get install zookeeperd -y
wget https://archive.apache.org/dist/kafka/2.4.1/kafka_2.11-2.4.1.tgz
mkdir /opt/kafka
tar -xvf kafka_2.11-2.4.1.tgz -C /opt/kafka/
rm kafka_2.11-2.4.1.tgz
nohup /opt/kafka/kafka_2.11-2.4.1/bin/kafka-server-start.sh /opt/kafka/kafka_2.11-2.4.1/config/server.properties &
wget -qO - https://artifacts.elastic.co/GPG-KEY-elasticsearch | sudo apt-key add -
sh -c 'echo "deb https://artifacts.elastic.co/packages/7.x/apt stable main" > /etc/apt/sources.list.d/elastic-7.x.list'
apt update
apt install elasticsearch
systemctl enable elasticsearch.service
systemctl start elasticsearch.service
update-rc.d elasticsearch defaults 95 10
apt install kibana
systemctl restart kibana
systemctl enable kibana
update-rc.d kibana defaults 96 9
wget http://gta.ufrj.br/~chagas/dataset-ids.tar.gz
tar -xvf dataset-ids.tar.gz
rm dataset-ids.tar.gz
cd dataset-ids
. ~/.bashrc
hdfs dfsadmin -safemode leave
hdfs dfs -put Network* /user/app
cd ..
pip install netifaces
apt-get install python-libpcap -y
pip install scapy
pip install kafka-python
apt install git -y
apt install net-tools -y
git clone https://github.com/GTA-UFRJ-team/gta-ids.git --branch stream
apt install curl -y
echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo apt-key add
apt-get update
apt-get install sbt
