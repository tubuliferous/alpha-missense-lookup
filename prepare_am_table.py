import pandas as pd
from sqlalchemy import create_engine, text
import gzip
import time
import numpy as np
import psycopg2

# filename = "AlphaMissense_hg38.tsv.gz_backup.gz"
alpha_missense_df = pd.read_csv(filename, sep="\t", compression='gzip', skiprows=3).rename(columns={"#CHROM": "CHROM"})
alpha_missense_df.head()


# Define a function to split the attributes
def split_attributes(attr):
    attr_dict = {}
    for item in attr.split(";"):
        item = item.strip()
        if item:
            key, value = item.split(" ", 1)
            attr_dict[key] = value.strip('"')
    return attr_dict

gtf_filename = "../AlphaMissense_data/Homo_sapiens.GRCh38.110.gtf.gz"
# Read the gzipped data into a dataframe, skipping comment lines
gtf_df = pd.read_csv(gtf_filename, sep="\t", header=None, comment="#")
# Rename columns
gtf_df.columns = ["seqname", "source", "feature", "start", "end", "score", "strand", "frame", "attributes"]
# Split the attributes column into separate columns
attributes_df = gtf_df['attributes'].apply(split_attributes).apply(pd.Series)
gtf_df = pd.concat([gtf_df.drop(columns=["attributes"]), attributes_df], axis=1)
gtf_transcripts_df = gtf_df[gtf_df['feature'].astype(str).str.contains("transcript")]

# Strip version numbers after the dot for the transcript_id column in alpha_missense_df
alpha_missense_df['transcript_id'] = alpha_missense_df['transcript_id'].str.split(".").str[0]

with pd.option_context('display.max_rows', 5, 'display.max_columns', None): 
    display(gtf_df)

# Merge the dataframes on the relevant keys
merged_df = alpha_missense_df.merge(gtf_transcripts_df[['gene_id', 'gene_name', 'transcript_id', 'transcript_name', 'start', 'end']], 
    on='transcript_id', 
    how='left')


# Convert "start" and "end" columns of merged_df to ints
merged_df['start'] = merged_df['start'].astype('Int64')
merged_df['end'] = merged_df['end'].astype('Int64')
merged_df.tail()

# merged_df.to_csv('AlphaMissense_hg38_with_genes', sep='\t', index=False)
merged_df = pd.read_csv('AlphaMissense_hg38_with_genes.tsv', sep='\t')
# merged_df = merged_df.drop(merged_df.columns[0], axis=1) # Remove index column
# tiny_df = merged_df.head()
merged_df.dtypes

# Bring in gene-level pathogenicity scores
am_gene_scores = pd.read_csv('AlphaMissense_gene_hg38.tsv.gz', sep='\t', compression='gzip', skiprows=3)
# Drop dot extentsion on transcript_ids
am_gene_scores['transcript_id'] = am_gene_scores.transcript_id.str.split('\.').str.get(0)
am_gene_scores

# Add gene-level pathogenicity scores to merged table
pd.merge(merged_df, am_gene_scores, )
merged_df = merged_df.merge(am_gene_scores[['transcript_id', 'mean_am_pathogenicity']], on='transcript_id', how='left')
# Rearrange columns
merged_df = merged_df[['CHROM', 'POS', 'REF', 'ALT', 'genome', 'am_pathogenicity', 'am_class', 'mean_am_pathogenicity', 'gene_name', 'gene_id', 'uniprot_id', 'transcript_id', 'protein_variant', 'transcript_name', 'start', 'end']]
merged_df.to_csv('AlphaMissense_hg38_annotated.tsv.gz', compression='gzip', index=False, sep='\t')


# ---- AWS Postgres Database Stuff ----
DATABASE_URL = 'postgresql://tubuliferous:Thelonius_91_starship@alpha-missense.cmoi1ddmzgx7.us-east-1.rds.amazonaws.com:5432/AlphaMissense'
engine = create_engine(DATABASE_URL)

# You can uncomment the below lines if you want to truncate data before insertion
# with engine.connect() as connection:
#     connection.execute(text("TRUNCATE deepmind_data;"))

filename = "AlphaMissense_hg38_annotated.tsv.gz"
upload_df = pd.read_csv(filename, sep="\t", compression='gzip')
# Insert data from DataFrame into PostgreSQL
chunk_size = 10000  # Adjust this based on your preference
total_chunks = len(upload_df) // chunk_size + 1

start_time = time.time()
for i, chunk in enumerate(np.array_split(upload_df, total_chunks)):
    chunk.to_sql('alpha_missense_data', engine, if_exists='append', index=False)
    print(f"Inserted chunk {i+1} of {total_chunks}")
end_time = time.time()
elapsed_time = end_time - start_time
print(f"The process took {elapsed_time:.2f} seconds.")


# # Fetch the first few rows (e.g., 5 rows) of the table and display them
table_name = "alpha_missense_data"
sample_df = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 10000", engine)
sample_df

