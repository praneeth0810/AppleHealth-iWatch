# Apple Health Data Pipeline

## 📑 Description

![Alt text](iwatch_health_architecture.png)

The **Apple Health Data Pipeline** processes health data exported from Apple Watch via iCloud, transforms the data using AWS services, and visualizes it through a **Streamlit dashboard**. This project leverages AWS EC2 for data processing, S3 for data storage, AWS Glue for data cataloging, and Amazon Athena for querying transformed data.

The pipeline is designed for scalability, efficiency, and ease of use, allowing users to seamlessly transform and visualize their health data without manual intervention.

---

## 🌟 Project Overview

### Objective
The goal of this project is to create an automated pipeline that processes raw Apple Watch health data into a structured format, making it easily accessible for analysis and visualization through an interactive dashboard.

### Core Features
1. **Automated Data Processing:**
   - From XML to structured CSV and Parquet formats.
   - Real-time and batch processing on AWS EC2.

2. **Efficient Data Storage and Querying:**
   - Raw, processed, and transformed data are stored in AWS S3.
   - Data is cataloged using AWS Glue and queried via Amazon Athena.

3. **Dynamic Health Dashboard:**
   - Visualize metrics such as heart rate, steps, sleep duration, and respiration rate.
   - Uses Streamlit for an interactive web interface.

4. **Unified Execution:**
   - Run the entire pipeline with a single command using `run.sh`.

---

## 🛠️ Tech Stack

### 1. AWS Services:
- **EC2:** Data processing and dashboard hosting.
- **S3:** Stores raw, processed, and transformed data.
- **AWS Glue:** Metadata cataloging for transformed data.
- **Athena:** SQL queries on transformed data.

### 2. Python Libraries:
- **Boto3:** Interface for AWS services.
- **Pandas:** Data transformation and aggregation.
- **Streamlit:** Building interactive dashboards.
- **Altair and Plotly:** Data visualization.
- **ElementTree:** Parsing XML data.
- **Logging:** Capturing script execution logs.

### 3. Bash Automation:
- **run.sh:** A script to execute the entire pipeline with one command, ensuring smooth and error-free execution.

---

## 🔄 Workflow

### Step 1: Data Export from Apple Watch
- Export data from Apple Health via iCloud.
- Upload the exported XML file to **S3 Raw Data** bucket.

### Step 2: Data Processing (EC2)
- The **extract_data.py** script:
  - Downloads XML from S3.
  - Parses and extracts metrics (heart rate, steps, sleep, respiration).
  - Saves processed data as CSV to **S3 Processed Data** bucket.

### Step 3: Data Transformation (EC2)
- The **transform_data.py** script:
  - Reads processed CSV from S3.
  - Transforms data to Parquet format for efficient querying.
  - Uploads transformed data to **S3 Transformed Parquet** bucket.

### Step 4: Data Cataloging and Querying (AWS Glue & Athena)
- Glue Crawler scans and catalogs the transformed data.
- Query the data using Amazon Athena for insights.

### Step 5: Visualization (Streamlit Dashboard)
- The **health_dashboard.py** script displays health metrics on an interactive dashboard.

---

## 💻 Run the Pipeline with One Command

To simplify execution, I have created a `run.sh` file that automates the entire pipeline.

### Usage:
1. Make the script executable:
   ```bash
   chmod +x run.sh
   ```
2. Run the pipeline:
   ```bash
   ./run.sh
   ```
3. The script will:
   - Run the extraction process.
   - Transform the data.
   - Launch the Streamlit dashboard.

### Why Use the run.sh Script?
- **Unified Execution:** Simplifies running multiple scripts with a single command.
- **Error Handling:** Stops the pipeline if any script fails.
- **Automatic Logging:** Captures the entire process, making debugging easier.
- **Efficiency:** Minimizes manual intervention and ensures a smooth workflow.

---

## 📊 Streamlit Dashboard

The **Streamlit Dashboard** provides a user-friendly interface to explore health data metrics.

### Dashboard Tabs:
1. **Heart Rate Monitoring:**
   - Daily average heart rate and zone breakdown.
   - Interactive line chart of heart rate trends.

2. **Step Count Analysis:**
   - Daily step counts and goal achievement.
   - Weekly step trend analysis.

3. **Sleep Duration Insights:**
   - Daily sleep duration and weekly averages.
   - Identifies best sleep nights.

4. **Respiration Rate Tracking:**
   - Daily average respiration rate.
   - Detects abnormalities in breathing patterns.

### Dashboard Features:
- **Interactive Filtering:** Choose the year and month to visualize.
- **Charts:** Line, bar, and pie charts for various metrics.
- **Alerts:** Displays warnings for abnormal metrics.

### Starting the Dashboard:
```bash
streamlit run scripts/health_dashboard.py
```

