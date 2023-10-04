import pandas as pd
from sqlalchemy import create_engine, text
import gzip
import time
import numpy as np

# Load data into a DataFrame
    # df = pd.read_csv(f, delimiter='\t', skiprows=2)
# filename = "AlphaMissense_hg38_with_genes.tsv.gz"
filename = "AlphaMissense_hg38_annotated.tsv.gz"
# filename = "test_table.tsv.gz"
# df = pd.read_csv(filename, sep="\t", compression='gzip', skiprows=3).rename(columns={"#CHROM": "CHROM"})
df = pd.read_csv(filename, sep="\t", compression='gzip')

# Connect to the PostgreSQL database
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')
DATABASE_URL = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"
# DATABASE_URL = 'postgresql://dash:dash@localhost:5432/am_database'k
engine = create_engine()

with engine.connect() as connection:
    connection.execute(text("DROP alpha_missense_data;"))

# Insert data from DataFrame into PostgreSQL
chunk_size = 10000  # Adjust this based on your preference
total_chunks = len(df) // chunk_size + 1

start_time = time.time()
for i, chunk in enumerate(np.array_split(df, total_chunks)):
    chunk.to_sql('alpha_missense_data', engine, if_exists='append', index=False)
    print(f"Inserted chunk {i+1} of {total_chunks}")
end_time = time.time()
elapsed_time = end_time - start_time
print(f"The process took {elapsed_time:.2f} seconds.")


sample_df = pd.read_sql("SELECT * FROM alpha_missense_data LIMIT 10000", engine)
sample_df
