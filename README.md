# Adsremedy Data Engineering Assignment 2

## Project Overview
This project implements a data engineering pipeline designed for the Adsremedy Assignment 2. The solution focuses on efficient data ingestion and storage using a NoSQL architecture.

## Tech Stack
* **Database:** MongoDB (Community Edition)
* **Containerization:** Docker
* **Language:** Python 3.x
* **Libraries:** [e.g., PyMongo, Pandas]

## Architectural Decisions: Why MongoDB?
Originally, this project was scoped for ScyllaDB. However, during development, I transitioned to **MongoDB** for the following engineering reasons:

1.  **Resource Efficiency:** ScyllaDB's architecture requires significant dedicated RAM and CPU to function correctly. To maintain system stability and ensure a smooth development environment on standard hardware, MongoDB was selected for its smaller memory footprint.
2.  **Flexibility:** MongoDB’s document-oriented model allowed for faster iteration on the data schema during the assignment phase.
3.  **Performance:** For the scope of this assignment, MongoDB provides excellent read/write throughput without the high overhead of a wide-column store like Scylla.

## Getting Started

### Prerequisites
* Docker & Docker Compose
* Python 3.9+

### Setup & Installation
1.  **Clone the Repository:**
    ```bash
    git clone git@github.com:dinkleva/adsremedy-2nd-assignment.git
    cd adsremedy-2nd-assignment
    ```

2.  **Launch the Environment:**
    The project uses a lightweight MongoDB container:
    ```bash
    docker-compose up -d
    ```

3.  **Change permission so that airflow can talk to spark container and it's internal files:**
    ```bash
    sudo chmod 666 /var/run/docker.sock
    ```

4.  **Final step to check if data is written to mongodb.**
    ```bash
    docker exec -it mongodb mongosh adsremedy --eval "db.daily_customer_totals.find().limit(5)"
    ```

## Project Structure
```text
├── docker-compose.yml  # MongoDB service configuration
├── data/               # Fake data and delta-table goes in this directory
├── dags/                # Orchestration script
├── scripts/             # Scripts for fake data generation, reading fake data and creating delta-table, and ETL script
├── .gitignore          # Properly configured to ignore data/ and .env
└── README.md           # Project documentation