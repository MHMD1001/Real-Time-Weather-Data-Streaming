# Real-Time-Weather-Data-Streaming
The project aims to demonstrate a data streaming pipeline using Apache Kafka for data ingestion, Apache Spark for real-time data processing, and Apache Hive for data storage and querying. The pipeline fetches weather data from the OpenWeatherMap API, processes it in real-time using Spark, and stores the processed data in Hive for further analysis and visualization.


## Architecture

![38f0a543-5c4e-4521-8239-ab9065a1db8d](https://github.com/MHMD1001/Real-Time-Weather-Data-Streaming/assets/113826367/52150bec-d0f3-4933-bccc-46c29399b0af)

The project architecture consists of several components:

#### Producer: 
Python notebook that fetches weather data from the OpenWeatherMap API and produces it to a Kafka topic.

#### Kafka:
Apache Kafka serves as the messaging system for data ingestion, where weather data is produced by the producer script and consumed by Spark.

#### Consumer:
Python script using PySpark that consumes data from Kafka, processes it, and writes it to both HDFS and Hive.

#### HDFS:
Hadoop Distributed File System (HDFS) is used for storing raw data.

#### Spark:
Apache Spark is employed for real-time data processing. It consumes data from Kafka, performs transformations, and writes the processed data to both HDFS and Hive.

#### Hive:
Apache Hive is used as a data warehouse. It stores the processed weather data in tabular form, making it queryable using SQL-like syntax.

#### Superset:
Apache Superset is an open-source data visualization tool. It connects to Hive to create interactive dashboards and visualizations based on the stored weather data.


## Workflow
The producer script fetches weather data from the OpenWeatherMap API and produces it to a Kafka topic named "weather_data".
Spark continuously consumes data from the "weather_data" Kafka topic using structured streaming.
Spark processes the data, performs necessary transformations, and writes the processed data to both HDFS and Hive.
The Hive table is created using a predefined schema, allowing easy querying and analysis of the stored weather data.
Apache Superset connects to Hive to visualize the weather data, allowing users to create interactive dashboards and visualizations.


## Project Files

1. **Dockerfile (Dockerfile)**:
   - Defines instructions for extending the base Jupyter/pyspark image to include the required Spark version.
   - Copies Spark binaries from the "spark-3.5.1-bin-hadoop3" folder into the image and sets appropriate environment variables.

2. **Docker Compose Configuration (docker-compose.yml)**:
   - YAML configuration file defining Docker services required for the project.
   - Includes services for Kafka, Hadoop, Hive, Spark, and Jupyter/pyspark.

3. **Jupyter Notebooks (producer.ipynb, consumer.ipynb)**:
   - Jupyter notebook files containing producer and consumer scripts, respectively.
   - Provides an interactive environment for running and testing the scripts.

4. **Hive Environment File (hive.env)**:
   - Environment file containing configurations for the Hive cluster.
   - Used when starting Docker containers to configure Hive services.

5. **Hadoop-Hive Environment File (hadoop-hive.env)**:
   - Environment file containing configurations for both Hadoop and Hive clusters.
   - Used when starting Docker containers to configure Hadoop and Hive services.

6. **Spark Binaries Folder (spark-3.5.1-bin-hadoop3)**:
   - Folder containing Spark binaries used to extend the Jupyter notebook image.
   - Dockerfile copies these binaries into the image to ensure compatibility with the Spark cluster.

7. **README.md**:
   - Markdown file providing instructions for setting up and running the project.
   - Includes information on installing dependencies, running Docker containers, and connecting services.


## Project Setup Steps
Before beginning this project, make sure you have the following prerequisites:
1- A machine with a minimum of 12GB of RAM available.
2- Docker installed on your machine.
3- Docker Compose installed on your machine.

### Follow these steps to set up to run the project:
Clone this repository to your local machine:

```bash
  git clone https://github.com/MHMD1001/Real-Time-Weather-Data-Streaming
```


### Step 1 (deploying the infrastructure):
Before deploying the infrastructure using docker-compose, we need to extend the base Jupyter image to include the required Spark version. This step ensures that our development environment is equipped with the necessary tools for working with Spark.

We start by extending the jupyter/pyspark-notebook image to incorporate the Spark version compatible with our Spark cluster. This extension process involves copying the Spark binaries into the image and configuring the necessary environment variables.

To extend the Jupyter image, navigate to the project directory and execute the following command:
```bash
  docker-compose  build jupyter-notebook --build-arg TAG_NAME=jupyter/pyspark-spark3.5.1
```
This command builds the Docker image using the Dockerfile provided in the project directory and tags it with the name jupyter/pyspark-spark3.5.1

Now we can deploy the infrfastructure using the following command:
```bash
  docker-compose up
```
if you don't want to keeps the current terminal occupied and stop streaming the logs of all containers to it, you can use the following command:
```bash
  docker-compose up -d
```


### Step 2 (Copy Jupyter Notebooks to Jupyter Container):
Kafka Producer and Consumer Notebooks
The Kafka producer and consumer scripts are provided as Jupyter notebooks (producer.ipynb and consumer1.ipynb, respectively) for easy interaction and execution.

After depolying the docker compose, copy the provided notebooks (producer.ipynb and consumer1.ipynb) to the Jupyter container using the following command:
```bash
  docker cp ./notebooks/producer.ipynb ./notebooks/consumer1.ipynb jupyter-notebook:/home/jovyan
```
This command copies the notebooks into the /home/jovyan directory inside the Jupyter container, allowing you to access and run them within the Jupyter notebook environment.


### Step 3 (Run Notebooks):
#### 1- Access Jupyter Notebook
Open your web browser and go to http://localhost:8888 to access the Jupyter notebook interface. You should now see the copied notebooks (producer.ipynb and consumer1.ipynb) listed in the directory.

#### 2- Run Kafka Producer Notebook
Open the producer.ipynb notebook in the Jupyter interface.
Follow the instructions within the notebook to execute the Kafka producer script.
This script will continuously fetch weather data from the OpenWeatherMap API and stream it to the Kafka topic.

Once the producer begins sending data to Kafka, you can access the Kafka Control Center UI using the URL "http://localhost:9021". Navigate to Brokers -> Topics, select the topic to which data is being sent, and proceed to Messages. Here, the Control Center actively listens to the Kafka Producer, allowing you to observe the data flow in real-time. This enables you to verify that the data is correctly transmitted to the Kafka Server and inspect its format, data types, and other relevant attributes.

#### 3- Run Kafka Consumer Notebook
Open the consumer1.ipynb notebook in the Jupyter interface.
Follow the instructions within the notebook to execute the Kafka consumer script.
This script will consume data from the Kafka topic, parse it, process it, and write it to a Hive table for further analysis.


### Step 4 (Setting Up Apache Superset):
For data visualization and dashboarding, Apache Superset can be integrated into the project. Below are the steps to set up Apache Superset using Docker Compose:
1- In "main" directory, Clone the Apache Superset GitHub repository to access the Docker Compose configuration and necessary files using the following command
```bash
  git clone https://github.com/apache/superset.git
```

2- Modify Docker Compose Configuration:
Navigate to the docker-compose.yml file in the cloned Superset repository. Add the following network configuration under the networks section to ensure that Superset runs on the same network as the main infrastructure services, including Hive server:
```yaml
networks:
  default:
    external:
      name: main_default
```

3- Modify the superset Service Image:
Update the image attribute in the superset service to apache/superset:latest in the Docker Compose configuration file. This ensures that Superset runs with the latest version of the image, resolving any issues with the UI.

4- Start Superset Using Docker Compose:
Run the following command to start Superset along with its dependencies using Docker Compose. Ensure that the main infrastructure Docker Compose is running to ensure that the main_default network exists:
```bash
cd superset
docker-compose up -d
```
This command will launch Superset services on the same network as the main infrastructure services, facilitating communication between Superset and Hive.


### Step 5 (Connect Superset to Hive):
After setting up Apache Superset and ensuring it is running, follow these steps to connect Superset to Hive:

1- Open Apache Superset UI by navigating to localhost:8088 in your web browser.

2- Log in using the default credentials:
Username: admin
Password: admin

3- Once logged in, click on the plus sign icon (+) in the top-right corner of the screen.

4- From the dropdown menu, select Data > Connect Database.

5- In the SUPPORTED DATABASES dropdown window, choose Apache Hive.

6- In the SQLALCHEMY URI field, type the following connection string:
"hive://hive@hive-server:10000/default"

7- Click on the Connect button to establish the connection.

Now Apache Superset is connected to your Apache Hive instance, and you can start creating datasets using Hive data and utilize them to generate visualizations and dashboards within Superset.




#### This README file provides comprehensive instructions for setting up and running the real-time weather data streaming project using Docker and various Apache technologies. Let me know if you need any further assistance or modifications!



