# Real-Time-Voting-Analytics
Welcome to the Realtime Election Voting System project! This project aims to simulate a real-world election scenario where votes are cast, processed, and visualized in real-time. Leveraging modern technologies like Docker, Kafka, PostgreSQL, Python, Apache Spark, and Streamlit, we've built a robust, scalable, and live-updating voting platform.

## System Components
main.py: Creates required tables on PostgreSQL, sets up Kafka topic, and manages data flow.
voting.py: Generates voting data and produces it to Kafka topic.
spark-streaming.py: Enriches data from PostgreSQL, aggregates votes, and produces data to specific Kafka topics.
streamlit-app.py: Consumes aggregated voting data and displays it in real-time using Streamlit.

## Setting up the System

->Prerequisites

1)Python 3.9 or above installed on your machine

2)Docker Compose installed on your machine

3)Docker installed on your machine

->Steps to Run

1)Clone this repository.

2)Navigate to the root containing the Docker Compose file.

3)Run the following command:

