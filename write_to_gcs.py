import csv
import os
from google.cloud import storage
import random
import requests

BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ["Management Party", "Savior Party", "Tech Republic Party"]
random.seed(42)


def generate_candidate_data(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            "candidate_id": user_data['login']['uuid'],
            "candidate_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "party_affiliation": PARTIES[candidate_number % total_parties],
            # "biography": "A brief bio of the candidate.",
            # "campaign_platform": "Key campaign promises or platform.",
            "photo_url": user_data['picture']['large']
        }
    else:
        return "Error fetching data"


def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]
        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration_number": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "cell_number": user_data['cell'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        }
    else:
        return "Error fetching data"


# Function to write candidate data to CSV file
def write_candidates_to_csv(candidates_data, file_name):
    with open(file_name, 'w', newline='') as csvfile:
        fieldnames = ['candidate_id', 'candidate_name', 'party_affiliation', 'photo_url']
        # 'biography', 'campaign_platform',
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for candidate in candidates_data:
            writer.writerow(candidate)


# Function to write voter data to CSV file
def write_voters_to_csv(voters_data, file_name, k):
    with open(file_name, 'w', newline='') as csvfile:
        fieldnames = ['voter_id', 'voter_name', 'date_of_birth', 'gender', 'nationality', 'registration_number',
                      'address_street', 'address_city', 'address_state', 'address_country', 'address_postcode',
                      'email', 'phone_number', 'cell_number', 'picture', 'registered_age']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Shuffle the voter data to introduce randomness
        random.shuffle(voters_data)

        # Initialize counters for K-anonymity
        anonymized_count = 0
        i = 0
        current_anonymized_group = []

        for voter in voters_data:
            # Flatten the address dictionary
            flat_voter = voter.copy()  # Create a copy of the voter dictionary
            address = flat_voter.pop('address')  # Remove the 'address' key from the copied dictionary
            for key, value in address.items():
                flat_voter[f'address_{key}'] = value  # Add flattened address fields to the copied dictionary

            if len(current_anonymized_group) >= (k - 1):
                # If the current anonymized group is filled, write the anonymized records to the CSV
                for anonymized_voter in current_anonymized_group:
                    writer.writerow(anonymized_voter)
                current_anonymized_group = []  # Reset the current group
                anonymized_count = 0  # Reset the anonymized count
                i += 1

            # Replace sensitive attributes with placeholders
            anonymized_voter = flat_voter.copy()
            anonymized_voter['voter_name'] = f'Voter {random.randint(1, 100)}'
            # anonymized_voter['address_state'] = 'Anonymous' + str(i)
            anonymized_voter['email'] = str(i) + 'anonymous@example.com'
            anonymized_voter['phone_number'] = '000-0000-000' + str(i)
            anonymized_voter['cell_number'] = '000-0000-000' + str(i)
            anonymized_voter['picture'] = str(i) + 'anonymous.jpg'

            current_anonymized_group.append(anonymized_voter)
            anonymized_count += 1

        # Write any remaining anonymized records to the CSV
        for anonymized_voter in current_anonymized_group:
            writer.writerow(anonymized_voter)


# Function to upload file to Google Cloud Storage
def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "real-time-voting-419520-cca679329a8f.json"
    # Initialize GCS client
    storage_client = storage.Client()

    # Get bucket
    bucket = storage_client.bucket(bucket_name)

    # Create blob object
    blob = bucket.blob(destination_blob_name)

    # Upload file to GCS
    blob.upload_from_filename(source_file_name)

    print(f'File {source_file_name} uploaded to GCS bucket {bucket_name} as {destination_blob_name}')


if __name__ == "__main__":

    # Create a list to store candidate data
    candidates_data = []
    bucket_name = "voting-dataset"

    # Generate and store candidate data in the list
    for i in range(3):
        candidate = generate_candidate_data(i, 3)
        candidates_data.append(candidate)

    # Write candidate data to CSV file
    candidates_file_name = 'candidates.csv'
    write_candidates_to_csv(candidates_data, candidates_file_name)

    # Upload candidates CSV file to Google Cloud Storage
    upload_to_gcs(bucket_name, candidates_file_name, candidates_file_name)

    # Create a list to store voter data
    voters_data = []

    # Generate and store voter data in the list
    for i in range(1000):
        print(i)
        voter_data = generate_voter_data()
        voters_data.append(voter_data)

    # Write voter data to CSV file
    voters_file_name = 'voters.csv'
    k_anonymity = 5  # Set the value of K for anonymity
    write_voters_to_csv(voters_data, voters_file_name, k_anonymity)

    # Upload voters CSV file to Google Cloud Storage
    upload_to_gcs(bucket_name, voters_file_name, voters_file_name)
