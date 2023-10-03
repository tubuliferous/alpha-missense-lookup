import pandas as pd
from sqlalchemy import create_engine, text
import gzip
import time
import numpy as np

# Load data into a DataFrame
    # df = pd.read_csv(f, delimiter='\t', skiprows=2)
filename = "AlphaMissense_hg38.tsv.gz_backup.gz"

df = pd.read_csv(filename, sep="\t", compression='gzip', skiprows=3).rename(columns={"#CHROM": "CHROM"})

# Connect to the PostgreSQL database
engine = create_engine('postgresql://dash:dash@localhost:5432/am_database')

# with engine.connect() as connection:
#     connection.execute(text("TRUNCATE deepmind_data;"))

# Insert data from DataFrame into PostgreSQL

# Assuming df_large is your large DataFrame
chunk_size = 10000  # Adjust this based on your preference
total_chunks = len(df) // chunk_size + 1

start_time = time.time()
for i, chunk in enumerate(np.array_split(df, total_chunks)):
    chunk.to_sql('deepmind_data', engine, if_exists='append', index=False)
    print(f"Inserted chunk {i+1} of {total_chunks}")
end_time = time.time()
elapsed_time = end_time - start_time
print(f"The process took {elapsed_time:.2f} seconds.")


df.to_sql('deepmind_data', engine, if_exists='replace', index=False)


    
# df_sql = pd.read_sql("SELECT * FROM deepmind_data", engine)
sample_df = pd.read_sql("SELECT * FROM deepmind_data LIMIT 5", engine)
sample_df


