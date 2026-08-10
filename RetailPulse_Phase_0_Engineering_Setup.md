# RetailPulse Enterprise Data Platform

## Phase 0 --- Engineering Environment & Project Setup

## 1. Project Overview

**RetailPulse** is an enterprise-style retail data engineering platform
designed to simulate how transactional, operational, and event-driven
data moves from multiple source systems into an AWS data lake and
ultimately into Snowflake for analytics and enterprise reporting.

The platform is being built using production-oriented engineering
practices including containerized source systems, version control,
event-driven ingestion, distributed data processing, data quality
controls, security, monitoring, and cloud data warehousing.

### Target Architecture

``` text
SOURCE SYSTEMS
PostgreSQL | MySQL | Files / Events
            |
            v
     INGESTION LAYER
      Python / Lambda
            |
            v
          AWS S3
         DATA LAKE
            |
     Raw / Quarantine / Processed
            |
            v
     AWS GLUE / PYSPARK
            |
            v
          CURATED
            |
            v
         SNOWFLAKE
            |
     Enterprise DW
            |
            v
         Analytics
```

## 2. Engineering Environment

Before developing the data platform, a reproducible local engineering
environment was established.

  -----------------------------------------------------------------------
  Layer                   Technology              Responsibility
  ----------------------- ----------------------- -----------------------
  Cloud                   AWS                     Data lake and cloud
                                                  processing
                                                  infrastructure

  Source Control          GitHub                  Central project
                                                  repository

  Version Control         Git                     Source-code lifecycle

  Development             VS Code                 Primary engineering IDE

  Programming             Python 3.13             Ingestion, automation
                                                  and processing

  Exploration             Jupyter Notebook        Data exploration and
                                                  prototyping

  Linux Runtime           WSL 2 / Ubuntu          Linux environment on
                                                  Windows

  Containers              Docker Desktop          Reproducible
                                                  application/database
                                                  runtime

  Container Orchestration Docker Compose          Multi-container local
                                                  environment

  Database Client         DBeaver                 SQL development and
                                                  database administration
  -----------------------------------------------------------------------

## 3. Development Architecture

``` text
                         GitHub
                            ^
                            |
                       Git Push/Pull
                            |
                         VS Code
                            |
              +-------------+-------------+
              |             |             |
           Python          SQL          Docker
                                            |
                                          WSL 2
                                            |
                                     Linux Containers
                                            |
                                 +----------+----------+
                                 |                     |
                            PostgreSQL               MySQL
                                 ^                     ^
                                 |                     |
                                 +------- DBeaver -----+
```

Development occurs on Windows while database and application workloads
execute in Linux containers.

## 4. Source Control Strategy

GitHub repository:

``` text
retailpulse-enterprise-data-platform
```

Local workspace:

``` text
C:\Users\saite\aws-data-engineering\
└── retailpulse-enterprise-data-platform
```

The repository was cloned using Git and connected to VS Code.

### Engineering Workflow

``` text
Develop -> Local Test -> Git -> Commit -> Push -> GitHub -> Future CI/CD
```

The repository will be the single source of truth for application code,
infrastructure configuration, database schemas, transformations and
technical documentation.

## 5. Containerized Development Strategy

PostgreSQL and MySQL will run as Docker containers instead of being
installed directly on Windows.

Benefits:

-   Environment isolation
-   Reproducibility
-   Version-controlled configuration
-   Portability
-   Easier cleanup and recreation

The environment will be defined using `docker-compose.yml`.

``` text
Docker Compose
       |
       +---- PostgreSQL -> Transaction DB
       |
       +---- MySQL -> Product / Inventory DB
```

## 6. Linux Runtime --- WSL 2

Docker Desktop uses WSL 2 as its Linux runtime layer.

``` text
Windows -> WSL 2 -> Ubuntu -> Linux Kernel -> Docker Engine -> Linux Containers
```

Verified configuration:

``` text
Distribution : Ubuntu
WSL Version  : 2
Architecture : x86_64
```

## 7. Docker Runtime

Docker Desktop is successfully configured with the WSL 2 backend.

``` text
Docker Engine      : 29.7.2
Docker Compose     : 5.3.1
Container OS       : Linux
Architecture       : x86_64
Kernel             : Microsoft WSL2
```

No project containers have been deployed yet.

## 8. Database Development Environment

DBeaver Community Edition is installed as the database development and
administration client.

DBeaver will connect to PostgreSQL and MySQL containers for:

-   SQL development
-   Schema inspection
-   Query execution
-   Data validation
-   Troubleshooting

DBeaver does not host the databases; Docker will run the actual database
servers.

## 9. Python Development Environment

Python 3.13 (64-bit) is configured as the primary programming runtime.

Python will be used for:

-   Synthetic source-data generation
-   Database extraction
-   Batch ingestion
-   AWS SDK integration
-   Lambda functions
-   Data validation
-   Automation
-   Pipeline utilities
-   Testing

Jupyter Notebook will primarily support experimentation and exploratory
analysis. Production logic will be maintained as structured
Python/PySpark modules in Git.

## 10. AWS Foundation

AWS will provide the cloud infrastructure for the platform.

The AWS account and IAM environment are available and ready for
development.

The project will separate human administrative access from application
workload access. Dedicated least-privilege IAM roles will be created for
workloads such as Lambda, Glue, ECS and Snowflake integration.

Credentials and secrets will never be committed to GitHub.

## 11. Phase 0 Validation

  Component           Status
  ------------------- --------
  AWS Environment     READY
  IAM                 READY
  GitHub Repository   READY
  Git                 READY
  VS Code             READY
  Python              READY
  Jupyter             READY
  WSL 2 / Ubuntu      READY
  Docker Engine       READY
  Docker Compose      READY
  DBeaver             READY

**Phase 0 Status: COMPLETE**

## 12. Next Engineering Milestone --- Phase 1: Source Systems

``` text
                     RETAILPULSE SOURCE LAYER

                         Docker Compose
                              |
                  +-----------+-----------+
                  |                       |
                  v                       v
             PostgreSQL                  MySQL
                  |                       |
          Order Management        Product / Inventory
                  |                       |
        customers, orders        products, inventory
       order_items, payments      stores, suppliers
                  |                       |
                  +-----------+-----------+
                              |
                            Python
                         Data Generator
```

The source systems will be created from code, populated with realistic
business data, validated through DBeaver, and committed to GitHub before
AWS ingestion development begins.
