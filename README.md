# Data Engineering & Machine Learning Pipeline on GCP

## Overview
This project aims on developing a **scalable ETL & Machine Learning pipeline** using **Apache Airflow** on **Google Cloud Platform (GCP) using Google Compose**. The pipeline automates data extraction from Kaggle and stores in Google Storage, transformation using AirFlow, and storage in **BigQuery**, followed by **machine learning modeling** and store models inside Google Storage during deployment.

## Features
- **ETL Pipeline**: Data extraction, transformation (Pandas), and storage.
- **Machine Learning Workflow**: Training and evaluating ML models.
- **Scalability**: Optimized for large-scale data processing.
- **Cloud Automation**: Leveraging **Apache Airflow** for orchestration.
- **CI/CD**: Using **GitHub Actions** for automated deployments.

## Tech Stack
- **Google Cloud Platform (GCP)**: Cloud Storage, BigQuery, Compute Engine
- **Apache Airflow**: Task orchestration
- **Pandas**: Data processing
- **BigQuery**: Data warehouse
- **Machine Learning**: Scikit-learn
- **CI/CD**: GitHub Actions

## Installation
### Prerequisites
Ensure you have the following installed:
- Python 3.8+
- Apache Airflow
- Google Cloud SDK
- Git

### Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/Siddharth-14/Loblaw-Data-Science.git
   cd Loblaw-Data-Science
   ```
2. Set up a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Running the Jupyter Notebook
### Prerequisites
Ensure the virtual environment is activated before running the notebook.

### Steps to Run the Notebook
1. Navigate to the notebooks/ directory:
   ```bash
   cd notebooks
   ```
2. Launch Jupyter Notebook:
   ```bash
   jupyter notebook
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Open the `.ipynb` files and execute the cells in order to avoid errors.

### Note:- For my process flow I saved the processed data inside BigQuery however for your easy to run I have saved a copy of the processed data locally. Normally it is a bad practice but just for ease i have performed this extra step. My notebook code is written in a way that in case it does not find the credentials it will use the local data.

## Usage
- DAG in `dags/` directory to orchestrate data workflows.
- Run data analysis and ml scripts from `notebooks/`.
- Task 3 models are stored in `models/`
- CI/CD automation file for Github Actions in `.github/`
- Store transformed data in BigQuery for analytics, ml models and data analysis tasks.

## CI/CD Automation
- Push changes to GitHub to trigger GitHub Actions workflows.

## AI Assistance Disclosure
### Part A - Question 5
AI was utilized:
- AI Usage: Used AI tools for code generation and debugging.
- Progress and Functionality: The code with AI assistance is operational with all capabilities.

This disclosure ensures transparency in the use of AI tools as per the challenge rules and demonstrates a proactive approach to ethical AI usage.

## References
- [YouTube Video on Apache Airflow & GCP](https://www.youtube.com/watch?v=ZgTf523XM0g)
- [ANOVA, T-test, and Other Statistical Tests with Python](https://towardsdatascience.com/anova-t-test-and-other-statistical-tests-with-python-e7a36a2fdc0c/)

## Contributors
- **Siddharth**