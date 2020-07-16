yum update -y && \
hdfs dfs -mkdir -p /apps/hudi/lib && \
hdfs dfs -copyFromLocal /usr/lib/hudi/hudi-spark-bundle.jar /apps/hudi/lib/hudi-spark-bundle.jar && \
hdfs dfs -copyFromLocal /usr/lib/spark/external/lib/spark-avro.jar /apps/hudi/lib/spark-avro.jar && \
cd /usr/lib/spark/jars && \
sudo wget https://repo1.maven.org/maven2/com/databricks/spark-xml_2.11/0.9.0/spark-xml_2.11-0.9.0.jar