# **IUDX Analytics Pipeline**
## **FISKR (Fisker) Pipeline**

This data ingestion and analytics pipline is acronymed based on the components involved in it:

- [*RabbitMQ*](https://www.rabbitmq.com/) - The main consumer of IUDX data. Publishes data into RMQ Exchange where broker sends to appropriate queues for consumption into the databases and analytic blocks.
- [*Flink*](https://flink.apache.org/) - Distributed processing engine. Consumes the stream of data from RMQ Broker and sinks it into Kudu Tables.
- [*Kudu*](https://kudu.apache.org/) - Distributed data storage engine for faster analytics. The data stream is stored into column-oriented data store for OLTP purposes.
- [*Impala*](https://impala.apache.org/) - Massively Parallel Processing (MPP) SQL query engine. It is used to query Kudu tables using external table mapping.
- [*Superset*](https://superset.apache.org/) - Visualization engine with large scale distribution capabilities.

### **System Requirements**
Setting this application will require multiple servers and of different configurations depending on the scale and number of datasources.
- **RabbitMQ**:
  - **Memory** - Recommended 4 GB.
- **Flink**: 
  - Java 8 or 11.
  - **Memory** - Minimum 4 GB, Recommended 8 GB.
  - **Storage Space** - 30 GB recommended.
- **Kudu**:
  - **Memory** - Recommended 4 GB.
  - **Storage Space** - 52 GB recommended.
- **Impala**:
  - **Memory** - Recommended 8 GB.
  - **Storage Space** - 128 GB or more recommended, ideally 256 GB or more.
- **Superset**:
  - **Memory** - Recommended 8 GB.
  - **Storage Space** - 40 GB recommended.
### **Setup order**
RMQ -> Kudu -> Flink -> Impala -> Superset -> Apps

### **Setup**
In order to setup the FISKR pipeline, a lot of components need to be setup individually and in a proper sequence.

Usual deployments will have to follow the following sequence.

#### **RabbitMQ**
RabbitMQ needs to be setup before everything so that the other components do find the queues while starting up.

    cd scripts/start_services
    ./start_rmq.sh
#### **Kudu**
Kudu stores the streaming data into a column-oriented database. It needs to be started before starting flink job so that Flink is able to find Kudu masters running beforehand.

    cd scripts/start_services
    ./start_kudu.sh
#### **Flink**
Flink adds the RMQ consumer as source to pull the data and sinks it into the Kudu tables. A Flink job can be setup locally using maven.

    cd flink-connectors/FlinkStreamingKudu
    mvn clean package
    cp dataConfig.properties target/dataConfig.properties
    cd target
    java -jar IUDXAnalyticsExperiment-1.0-SNAPSHOT.jar

Also, this JAR file can be submitted as job to the Flink cluster. Its setup and code will be updated soon.

#### **Impala**
Impala is used to query into Kudu tables. It creates a mapping table (external table) which creates a link to the Kudu table. Then, SQL queries can be run in `impala-shell` to access Kudu tables' data.

    cd scripts/start_services
    ./start_impala.sh
#### **Superset**
