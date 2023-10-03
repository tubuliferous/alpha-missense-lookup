import os
import requests
import dash
from dash import dcc, html, Input, Output, dash_table
import pandas as pd
import dash_bootstrap_components as dbc
import threading
import logging
from sqlalchemy import create_engine
from sqlalchemy import text


logging.basicConfig(level=logging.INFO)

filename = "AlphaMissense_hg38.tsv.gz"
url = "https://zenodo.org/record/8208688/files/AlphaMissense_hg38.tsv.gz?download=1"
data_ready_event = threading.Event()

DATABASE_URL = 'postgresql://dash:dash@localhost:5432/am_database'  # Adjust this accordingly
engine = create_engine(DATABASE_URL)


def download_and_load_data():
    try:
        logging.info("Downloading AlphaMissense table...")
        if not os.path.exists(filename):
            response = requests.get(url, stream=True)
            with open(filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=1024):
                    if chunk:
                        file.write(chunk)
            if os.path.exists(filename):
                logging.info(f"{filename} has been downloaded.")
            else:
                logging.error(f"Failed to download {filename}.")
        else:
            logging.info(f"{filename} already exists.")
        
        logging.info("Loading AlphaMissense table into DataFrame...")
        df = pd.read_csv(filename, sep="\t", compression='gzip', skiprows=3).rename(columns={"#CHROM": "CHROM"})
        logging.info("Loading AlphaMissense table into postgres database...")
        df.to_sql('AlphaMissense', engine, if_exists='replace', index=False)
        logging.info("AlphaMissense table loaded into database.")
        data_ready_event.set()
    except Exception as e:
        logging.error("Error in download_and_load_data: %s", str(e))


# Initial setup
download_and_load_data()


with engine.connect() as connection:
    stmt = text("SELECT * FROM AlphaMissense LIMIT 10;")  # Replace 'your_table_name' with the actual table name you've used
    result = connection.execute(stmt)
    for row in result:
        print(row)

with engine.connect() as connection:
    result = connection.execute("SELECT tablename FROM pg_tables WHERE schemaname='public';")
    for row in result:
        print(row[0])





threading.Thread(target=download_and_load_data).start()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("AlphaMissense Lookup"), width={"size": 6, "offset": 3}), className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Label('Chromosome:'),
            dbc.Select(
                id="chrom-dropdown",
                options=[],
                value=None
            )
        ], width=2),
        dbc.Col([
            dbc.Label('Position:'),
            dbc.Input(id="position-input", type="number", placeholder="Enter position")
        ], width=2),
        dbc.Col([
            dbc.Label('Genotype:'),
            dbc.Input(id="genotype-input", type="text", placeholder="Enter genotype")
        ], width=2),
        dbc.Col([
            dbc.Button("Submit", id="submit-btn", color="primary", className="mt-4")
        ], width=2)
    ], className="mb-4"),
    dbc.Row([
        dbc.Col(
            dcc.Loading(
                id="loading",
                type="default",
                children=[
                    dash_table.DataTable(id="table-output")
                ]
            )
        )
    ])
], fluid=True)

app.clientside_callback(
    """
    function(n_submit, n_enter) {
        if(n_enter>n_submit) {
            return n_enter;
        } else {
            return n_submit;
        }
    }
    """,
    Output('submit-btn', 'n_clicks'),
    Input('submit-btn', 'n_clicks'),
    Input('genotype-input', 'n_submit')
)

@app.callback(
    Output("table-output", "data"),
    Output("chrom-dropdown", "options"),
    Output("chrom-dropdown", "value"),
    [Input("submit-btn", "n_clicks")],
    [dash.dependencies.State("chrom-dropdown", "value"),
     dash.dependencies.State("position-input", "value"),
     dash.dependencies.State("genotype-input", "value")]
)
def update_table(n_clicks, chrom, position, genotype):
    try:
        data_ready_event.wait()

        # Fetch unique chromosomes for dropdown
        chroms = engine.execute('SELECT DISTINCT CHROM FROM AlphaMissense').fetchall()
        options = sorted([{'label': chrom[0], 'value': chrom[0]} for chrom in chroms])

        initial_value = chroms[0][0] if not chrom else chrom
        
        # Fetch filtered data based on user input
        if not genotype:
            return [], options, initial_value

        genotype = genotype.upper()
        if len(genotype) == 1:
            query = '''SELECT * FROM AlphaMissense WHERE CHROM=%s AND POS=%s AND ALT=%s'''
            data = engine.execute(query, (chrom, position, genotype)).fetchall()
        else:
            query = '''SELECT * FROM AlphaMissense WHERE CHROM=%s AND POS=%s AND (ALT=%s OR ALT=%s)'''
            data = engine.execute(query, (chrom, position, genotype[0], genotype[1])).fetchall()

        # Convert data to dictionary format for DataTable
        columns = ["CHROM", "POS", "REF", "ALT", "genome", "uniprot_id", "transcript_id", 
                   "protein_variant", "am_pathogenicity", "am_class"]
        data_dicts = [dict(zip(columns, row)) for row in data]

        return data_dicts, options, initial_value
    except Exception as e:
        logging.error("Error in update_table callback: %s", str(e))
        return [], [], None

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))
    app.run_server(debug=False, host='0.0.0.0', port=port)
