import os
import csv
import json
import psycopg2
from confluent_kafka import SerializingProducer
from google.cloud import storage


# Function to read CSV file from GCS
def read_csv_from_gcs(bucket_name, file_name):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "real-time-voting-419520-cca679329a8f.json"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_string().decode('utf-8')
    return file_content.splitlines()


# Function to create tables in PostgreSQL
def create_tables(conn, cur):
    # biography TEXT,
    # campaign_platform TEXT,
    cur.execute("""
        CREATE TABLE IF NOT EXISTS candidates (
            candidate_id VARCHAR(255) PRIMARY KEY,
            candidate_name VARCHAR(255),
            party_affiliation VARCHAR(255),
            photo_url TEXT
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS voters (
            voter_id VARCHAR(255) PRIMARY KEY,
            voter_name VARCHAR(255),
            date_of_birth VARCHAR(255),
            gender VARCHAR(255),
            nationality VARCHAR(255),
            registration_number VARCHAR(255),
            address_street VARCHAR(255),
            address_city VARCHAR(255),
            address_state VARCHAR(255),
            address_country VARCHAR(255),
            address_postcode VARCHAR(255),
            email VARCHAR(255),
            phone_number VARCHAR(255),
            cell_number VARCHAR(255),
            picture TEXT,
            registered_age INTEGER
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            voter_id VARCHAR(255) UNIQUE,
            candidate_id VARCHAR(255),
            voting_time TIMESTAMP,
            vote int DEFAULT 1,
            PRIMARY KEY (voter_id, candidate_id)
        )
    """)

    conn.commit()


# Function to insert voters record into PostgreSQL
def insert_voters(conn, cur, voter):
    cur.execute("""
                        INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number, address_street, address_city, address_state, address_country, address_postcode, email, phone_number, cell_number, picture, registered_age)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s)
                        """,
                (voter["voter_id"], voter['voter_name'], voter['date_of_birth'], voter['gender'],
                 voter['nationality'], voter['registration_number'], voter['address_street'],
                 voter['address_city'], voter['address_state'], voter['address_country'],
                 ['address_postcode'], voter['email'], voter['phone_number'],
                 voter['cell_number'], voter['picture'], voter['registered_age'])
                )
    conn.commit()


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


if __name__ == "__main__":
    # Connect to PostgreSQL
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()

    # Kafka Topics
    voters_topic = 'voters_topic'

    # Initialize Kafka producer
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})

    # Create tables in PostgreSQL
    create_tables(conn, cur)

    # get candidates from db
    cur.execute("""
            SELECT * FROM candidates
        """)
    candidates = cur.fetchall()
    print(candidates)

    cur.execute("""
                SELECT * FROM voters
            """)
    voters = cur.fetchall()
    print(candidates)

    # biography, campaign_platform,
    # %(biography)s, %(campaign_platform)s,
    if len(candidates) == 0:
        # Read candidates.csv from GCS and insert candidates data into PostgreSQL
        candidates_file_content = read_csv_from_gcs("voting-dataset", "candidates.csv")
        for row in csv.DictReader(candidates_file_content):
            cur.execute("""
                INSERT INTO candidates (candidate_id, candidate_name, party_affiliation, photo_url)
                VALUES (%(candidate_id)s, %(candidate_name)s, %(party_affiliation)s, %(photo_url)s)
            """, row)
        conn.commit()

    if len(voters) == 0:

        # Loop through voters.csv from GCS, insert voters record into PostgreSQL, and create Kafka topic
        voters_file_content = read_csv_from_gcs("voting-dataset", "voters.csv")
        for row in csv.DictReader(voters_file_content):
            insert_voters(conn, cur, row)

            # Produce to Kafka topic
            producer.produce(
                voters_topic,
                key=row["voter_id"],
                value=json.dumps(row),
                on_delivery=delivery_report
            )
            producer.flush()

    # Close PostgreSQL connection
    cur.close()
    conn.close()
