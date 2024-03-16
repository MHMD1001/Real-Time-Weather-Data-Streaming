FROM jupyter/pyspark-notebook:latest

# Copy Spark 3.5.1 into the image
COPY ./spark-3.0.0-bin-hadoop3.2 /opt/spark

# Set Spark environment variables
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH


