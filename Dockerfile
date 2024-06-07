# Use the official OpenJDK base image
FROM openjdk:11-jdk-slim

# Install necessary packages
RUN apt-get update && apt-get install -y wget tar python3 python3-pip

# Download and install Apache Spark
ENV SPARK_VERSION=3.5.1
RUN wget -qO- https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz | tar xvz -C /opt/
RUN ln -s /opt/spark-3.5.1-bin-hadoop3 /opt/spark

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Install SQLite
RUN apt-get install -y sqlite3 libsqlite3-dev

# Install PySpark
RUN pip3 install pyspark

WORKDIR /sqlite_jdbc

COPY sqlite-jdbc-3.46.0.0.jar ./sqlite-jdbc-3.46.0.0.jar

WORKDIR /src

COPY generate_data.py ./generate_data.py

COPY spark_query.py ./spark_query.py

WORKDIR /src