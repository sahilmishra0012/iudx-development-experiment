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
