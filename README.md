# Data Warehouse & Analytics Project

Welcome to the **Data Warehouse & Analytics Project** repository.

This project demonstrates the design and implementation of a **modern data warehouse** using a **Medallion Architecture (Bronze–Silver–Gold)** approach. Built as a portfolio project, it showcases industry best practices in **data engineering, data modeling, and analytics**, using PostgreSQL as the warehouse technology.

---

## 📐 Data Architecture

The data architecture follows the **Medallion Architecture**, consisting of three logical layers:

![Medallion Architecture](https://github.com/user-attachments/assets/2076bf96-9e24-4158-b557-28ae0cd15144)

### 🥉 Bronze Layer — Raw Data
- Stores raw source data **as-is**
- Data ingested from CSV files
- Represents source systems without transformation
- Serves as the immutable ingestion layer

### 🥈 Silver Layer — Clean & Conformed Data
- Data cleansing, standardization, and normalization
- Business rules and transformations applied
- Data prepared for analytical modeling
- Ensures improved data quality and consistency

### 🥇 Gold Layer — Analytics-Ready Data
- Business-ready data modeled using a **star schema**
- Fact and dimension tables optimized for BI tools
- Designed for analytical queries and reporting
- Final consumption layer for stakeholders

---

## 📌 Project Overview

This project covers the full lifecycle of a data warehouse solution:

- **Data Architecture**
  - Designing a modern warehouse using Medallion Architecture
- **ETL Pipelines**
  - Extracting data from source systems
  - Transforming and cleansing data across layers
  - Loading curated datasets into analytics-ready models
- **Data Modeling**
  - Implementing fact and dimension tables
  - Designing a star schema optimized for analytics
- **Analytics & Reporting**
  - Writing SQL-based analytical queries
  - Enabling actionable business insights

---

## 🛠️ Technologies Used

- **Database:** PostgreSQL
- **Query Language:** SQL
- **Architecture Pattern:** Medallion (Bronze / Silver / Gold)
- **Data Modeling:** Star Schema
- **Source Format:** CSV files
- **Analytics:** SQL-based reporting

---

## 🎯 Project Requirements

### Building the Data Warehouse (Data Engineering)

**Objective**

Develop a modern data warehouse using PostgreSQL to consolidate sales-related data and enable analytical reporting and informed decision-making.

**Specifications**

- **Data Sources**
  - Two source systems (ERP and CRM)
  - Data provided as CSV files
- **Data Quality**
  - Data cleansing and validation prior to analytics
- **Integration**
  - Combine multiple sources into a unified analytical model
- **Scope**
  - Focus on the latest available data
  - No historization or slowly changing dimensions required
- **Documentation**
  - Clear documentation of the data model
  - Designed for both business stakeholders and analytics teams

---

## 📊 BI, Analytics & Reporting

In addition to building the data warehouse, this project includes
SQL-based analytics built on top of the Gold layer.

**Objective**

Develop SQL-based analytics to deliver insights into:

- Customer behavior
- Product performance
- Sales trends

These insights support stakeholders with **key business metrics**, enabling data-driven and strategic decision-making.


See the [`scripts/analytics`](./analytics) folder for:
- Exploratory data analysis
- Advanced analytical patterns
- Business KPIs and reporting views


---

## 💼 Skills Demonstrated

This project demonstrates hands-on experience in:

- Data Warehousing concepts
- Medallion Architecture design
- ETL pipeline development
- SQL-based data transformation
- Data quality management
- Dimensional data modeling
- Analytics-ready schema design

It is particularly relevant for roles such as:

- Data Engineer
- Analytics Engineer
- Data Architect
- ETL / Pipeline Developer
- Data Analyst

---

## 📝 Notes

- PostgreSQL is used as the warehouse technology for this project.
- In cloud data warehouses (e.g. Snowflake, BigQuery), similar architectures would typically be implemented using multiple databases.
- This project focuses on **core data engineering principles**, independent of specific vendor tooling.

---

## 👤 Author

Built by **Balázs Illovai** as part of a broader portfolio focused on transitioning toward **analytics engineering and data infrastructure roles**.
