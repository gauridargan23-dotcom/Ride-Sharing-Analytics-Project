# 🚗 Ride-Sharing Analytics & Driver Performance Pipeline

## 📌 Project Overview

This project implements an end-to-end **data engineering pipeline for ride-sharing analytics and driver performance analysis** using **PySpark** and the **Medallion Architecture**.

The pipeline processes raw driver, trip, and trip-log data through three layers:

- 🥉 **Bronze Layer** – Stores raw data without transformations
- 🥈 **Silver Layer** – Cleans, validates, joins, and enriches the data
- 🥇 **Gold Layer** – Generates business-level KPIs and analytical datasets

The final output helps analyze:

- Driver performance
- Trip completion and cancellation
- Driver rankings
- Trip delays
- High-demand pickup locations
- Revenue performance
- Overall ride-sharing KPIs

---

## 🎯 Project Objectives

The main objectives of this project are:

- Track ride and driver activity
- Analyze trip cancellations
- Analyze trip delays
- Identify high-demand pickup locations
- Evaluate driver performance
- Generate business-level KPIs
- Calculate revenue insights
- Rank drivers based on performance

---

## 🏗️ Architecture

The project follows the **Medallion Architecture**:

```text
                 ┌────────────────────┐
                 │   Raw CSV Files    │
                 │                    │
                 │  drivers.csv       │
                 │  trips.csv         │
                 │  trip_logs.csv     │
                 └─────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   🥉 BRONZE LAYER   │
                │                     │
                │ Raw data as-is      │
                │ Parquet format      │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │   🥈 SILVER LAYER   │
                │                     │
                │ Data Cleaning       │
                │ Data Validation     │
                │ Joins               │
                │ Null Handling       │
                │ Derived Columns     │
                └──────────┬──────────┘
                           │
                           ▼
                ┌─────────────────────┐
                │    🥇 GOLD LAYER    │
                │                     │
                │ Business KPIs       │
                │ Driver Performance  │
                │ Cancellation        │
                │ Demand Analysis     │
                │ Delay Analysis      │
                │ Revenue Analysis    │
                └─────────────────────┘
