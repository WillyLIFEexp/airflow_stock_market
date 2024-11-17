# Stock Market Analysis Pipeline
This is a practice for Stock Market ETL Pipeline extracts real-time or historical stock price data, transforms it into a clean, standardized format, and loads it into a data warehouse (e.g., Snowflake) for analysis.

## :hammer_and_pick: Technologies Used
- Programming Languages: Python
- Orchestration: Apache Airflow
- Data Warehouse: Snowflake
- Other Libraries: Pandas, Requests, SQLAlchemy

## :gear: Installation
- Python 3.9+
- Docker (for running Airflow)

## Steps
1. Running Airflow on docker using `docker compose up -d`
    - if need to add new python lib, we need to add the create requirement.txt and then change the setting inside the docker-compose.yml file.
2. After successfully build the docker, we need to add the connection to the Postgresql we will be using, click Admin -> Connection on the Airflow UI page to add the connection.
3. Start the dag and check if everything run successfully.

## Todo list
- Adding job to parse the data and then save it to new database.
- Create different dag for different purpose.
- Try different kind of schedule method.
