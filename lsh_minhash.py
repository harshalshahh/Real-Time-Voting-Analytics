import hashlib
import psycopg2
import numpy as np
from nltk.util import ngrams

class MinHash:
    def __init__(self, num_perm):
        self.num_perm = num_perm
        self.hash_funcs = self._generate_hash_funcs(num_perm)
        self.min_hash_values = None

    def _generate_hash_funcs(self, num_perm):
        hash_funcs = []
        for i in range(num_perm):
            hash_funcs.append(self._generate_hash_func())
        return hash_funcs

    def _generate_hash_func(self):
        seed = np.random.randint(0, 2**32 - 1)
        return lambda x: hashlib.md5((str(x) + str(seed)).encode()).hexdigest()

    def _min_hash(self, set_values):
        min_hash_values = []
        for hash_func in self.hash_funcs:
            min_hash_value = float('inf')
            for value in set_values:
                hash_value = int(hash_func(value), 16)
                if hash_value < min_hash_value:
                    min_hash_value = hash_value
            min_hash_values.append(min_hash_value)
        return min_hash_values

    def compute_hash_signature(self, candidate):
        shingles = set()
        for column_value in candidate:
            if isinstance(column_value, str):  # Only consider string values
                shingles.update(ngrams(column_value.split(), 3))
        return self._min_hash(shingles)

    def jaccard_similarity(self, signature1, signature2):
        num_perm = len(signature1)
        intersection = sum(int(signature1[i] == signature2[i]) for i in range(num_perm))
        union_size = num_perm

        # Check for zero division
        if union_size == 0:
            return 0.0

        jaccard_sim = intersection / union_size
        return jaccard_sim


if __name__ == "__main__":
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cur = conn.cursor()
    cur.execute("""SELECT * FROM candidates""")
    candidates = cur.fetchall()

    # Initialize MinHash object
    num_perm = 128
    minhash = MinHash(num_perm)

    # Compute hash signatures for candidates
    signatures = []
    for candidate in candidates:
        signature = minhash.compute_hash_signature(candidate)
        signatures.append(signature)

    # Calculate Jaccard similarity for all pairs of candidates
    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            jaccard_similarity = minhash.jaccard_similarity(signatures[i], signatures[j])
            print(f"Jaccard Similarity between candidate {candidates[i]} and candidate {candidates[j]}:",
                  jaccard_similarity)
