# SQL-Pilot  

## Overview  
SQL-Pilot is an AI-powered SQL assistant designed to simplify database management and query generation. It helps developers, analysts, and data engineers overcome challenges in schema design, SQL dialects, and query optimization.  

## Features  

### Core Features  
- **Schema Design Assistance**: Suggests optimal database schemas for OLAP use cases based on natural language descriptions.  
- **DDL and DML Generation**: Dynamically generates Data Definition Language (DDL) and Data Manipulation Language (DML) statements.  
- **SQL Completion Assistance**: Provides intelligent SQL code completion.  
- **Natural Language to SQL Translation**: Converts English text into SQL queries for Trino or Spark SQL dialects.  
- **Query Execution**: Executes SQL queries using Trino or Spark SQL as execution engines.  
- **Feedback Incorporation**: Learns from user feedback to improve query generation and schema recommendations over time.  

### Bonus Features  
- **Storage-Optimized Schema Suggestions**: Recommends schemas optimized for storage and performance.  

### Core Functionality  
- **Dynamic SQL Generation**: Supports Trino and Spark SQL dialects.  
- **Validation Environment**: Uses Trino docker execution environment for query validation.  
- **Interactive UI**:  
    - Input schema requirements.  
    - Generate and edit SQL statements.  
    - Execute queries.  
    - Provide feedback.  
- **Feedback Learning**: Refines schema and query suggestions based on user feedback.  

### Tech Stack  
- **Frontend**: Next.js, Shadcn UI.  
- **Backend**: LangChain, PySpark, Trino.  
- **Database**: MongoDB.  
- **Containerization**: Docker.  

## Getting Started  

### Prerequisites  
- Docker installed on your system.  
- MongoDB instance running.  

### Installation  
1. Clone the repository:  
     ```bash  
     git clone https://github.com/RajwardanMali/sql-pilot.git  
     cd sql-pilot  
     ```  
2. Build and run the Docker containers:  
     ```bash  
     docker-compose up --build  
     ```  
3. Access the application at `http://localhost:3000`.  

### Usage  
1. Input schema requirements in the UI.  
2. Generate and edit SQL statements using natural language query.  
3. Execute queries.  
4. Talk to your data using spark analytics.  
4. Provide feedback to improve future suggestions (backend).  



## Contact  
For questions or support, please contact [rajmali2104@gmail.com].  
