# Open Government Data Big Data Prototype

Prototype of a full stack Big Data solution for an Open Data Government portal.
The project contains a complete suit to process , save and show Raw data generated by the portal.
  - ETL:
          Apache Spark
          https://spark.apache.org/
  - Datawarehouse:
          Apache drill
          https://drill.apache.org/
  - Orchestration:
          Apache Airflow
          https://airflow.apache.org/
  - Dashboard :
          Dash plotly
          https://plot.ly/products/dash/

Each software is supported by Docker https://www.docker.com/

To test and show the prototype working, a complete example has been added with an Orchestred ETL , Datawarehouse and Dashboard example following the design guidelines included in the memory.
Both raw and processed data has been added in the data folder.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Prerequisites

To get the prototype running first of all is necessary to install the last version of Docker and Docker-compose either in Linux or Windows OS
- https://docs.docker.com/docker-for-windows/install/
- https://docs.docker.com/install/linux/docker-ce/ubuntu/
- https://docs.docker.com/compose/install/


### Installing

Download or clone the project located in https://github.com/aitor1990/tfm_BI_BIGDATA

- To clone the project via Git use:  
`git clone https://github.com/aitor1990/tfm_BI_BIGDATA.git`

Inside the root of the project a docker-compose file contains all the instructions needed to run all docker instances

- To build the project run the command  
`docker-compose build`

The installation may take a while as Docker must download all the dependencies and libraries needed to run the project.
Once downloaded, the dependencies will be cached in disk.

## Deployment

To deploy the project in a local machine via Docker run the command   

`docker-compose up`

After executing the command four docker instances will be executed at the same time.

Also if necessary each instance can be deployed individualy.

`docker-compose up airflow`  
`docker-compose up drill`  
`docker-compose up spark`  
`docker-compose up dashboard`


- #### Airflow
  An airflow server programmed with a Spark + Livy example ready to be launch. It has access, via http docker internal network, to the Apache livy server
  The Airflow server can be accessed via HTTP port 8081  
  Example: http://localhost:8081/admin/

- #### Spark
  A livy server connected to a spark installation over Hadoop
  To monitorize the livy sessions access the HTTP port 8080.  
  Example : http://localhost:8998/

- #### Drill
  A Drill embedded server, used as a datawarehouse, with access to the data folder.
  The administration console is accessible via HTTP port 8047.   
  Example : http://localhost:8047/

- #### Dashboard
  A Plotly Dash server example with access to the Drill instance via http internal docker network.
  Example : http://localhost:8050/

## Other consideration

The system is composed of two totally separated parts that can be executed and tested independently.
  - When developing or testing the ETL only spark and airflow must be deployed.
  - If only the dashboard is neededed then run both drill and dashboard instances.

The four instances running can be hardware hungry specially when executing the ETL. It is highly recommended when using a limited hardware to execute the ETL only with airflow and spark instances running.

To execute the DAG remember first of all change the switch botton on the left.
Then press the play button on the right (no date has been added for this demo)

When the ETL is launched via airflow the data processed previosuly is overwritten.

## Bugs

Docker-compose nowadays hasn't a method to execute the instances in an specific order. Although a system has been implemented to make the dashboard server wait until drill is ready, there has been some cases when the server has crashed and the instance has stopped. In this case rerun the dashboard instance.
