import os
import logging
from sqlalchemy import create_engine, text
import dash
from dash import dcc, html, Input, Output, dash_table
import dash_bootstrap_components as dbc
import pandas as pd

logging.basicConfig(level=logging.INFO)
db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_host = os.environ.get('DB_HOST')
db_name = os.environ.get('DB_NAME')

DATABASE_URL = f"postgresql://{db_user}:{db_pass}@{db_host}/{db_name}"

# DATABASE_URL = 'postgresql://dash:dash@localhost:5432/am_database'
engine = create_engine(DATABASE_URL)

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

@app.callback(
    [Output("table-output", "data"),
     Output("chrom-dropdown", "options"),
     Output("chrom-dropdown", "value")],
    [Input("submit-btn", "n_clicks")],
    [dash.dependencies.State("chrom-dropdown", "value"),
     dash.dependencies.State("position-input", "value"),
     dash.dependencies.State("genotype-input", "value")]
)
def update_table(n_clicks, chrom, position, genotype):
    # Fetch unique chromosomes for dropdown
    options = []
    try:
        with engine.connect() as connection:
            stmt = text('SELECT DISTINCT "CHROM" FROM alpha_missense_data')
            chroms = connection.execute(stmt).fetchall()
            options = sorted([{'label': chrom[0], 'value': chrom[0]} for chrom in chroms], key=lambda x: x['value'])
    except Exception as e:
        logging.error("Error fetching chromosome data: %s", str(e))

    # If the dropdown was previously empty or the selected value is not in the new options
    if not chrom or not any(opt['value'] == chrom for opt in options):
        chrom = options[0]['value'] if options else None

    data_dicts = []
    # Fetch filtered data based on user input
    if genotype:
        genotype = genotype.upper()
        # Dynamically construct the IN clause based on the genotype length
        in_clause = ', '.join([f':genotype{i}' for i in range(len(genotype))])
        with engine.connect() as connection:
            query = text(f'''SELECT * FROM alpha_missense_data WHERE "CHROM"=:chrom AND "POS"=:position AND "ALT" IN ({in_clause})''')
            # Construct the parameters dictionary
            params = {"chrom": chrom, "position": position}
            for i, gen in enumerate(genotype):
                params[f'genotype{i}'] = gen
            data = connection.execute(query, params).fetchall()

        # Convert data to dictionary format for DataTable
        columns = ["CHROM", "POS", "REF", "ALT", "genome", "uniprot_id", "transcript_id", 
                   "protein_variant", "am_pathogenicity", "am_class"]
        data_dicts = [dict(zip(columns, row)) for row in data]

    return data_dicts, options, chrom

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))
    app.run_server(debug=False, host='0.0.0.0', port=port)
