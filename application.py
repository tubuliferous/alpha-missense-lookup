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

print(f'{db_host}')

DATABASE_URL = f'postgresql://{db_user}:{db_pass}@{db_host}/{db_name}'

engine = create_engine(DATABASE_URL)

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("AlphaMissense Lookup"), width={"size": 6, "offset": 3}), className="mb-4"),
    dbc.Row([
        dbc.Col([
            dbc.Label('Chromosome:', style={"fontWeight": "bold"}),
            dbc.Select(
                id="chrom-dropdown",
                options=[],
                value=None
            )
        ], width=2),
        dbc.Col([
            dbc.Label('Position (hg38):', style={"fontWeight": "bold"}),
            dbc.Input(id="position-input", type="number", placeholder="Enter position", n_submit=0)
        ], width=2),
        dbc.Col([
            dbc.Label('Genotype (+ strand):', style={"fontWeight": "bold"}),
            dbc.Input(id="genotype-input", type="text", placeholder="Enter genotype", n_submit=0)
        ], width=2),
        dbc.Col([
            dbc.Button("Submit", id="submit-btn", color="primary", style={"margin-top": "32px"})
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

def chromosomal_order(chrom):
    """Function to give chromosomes a natural sort order."""
    # Remove 'chr' prefix if it exists
    chrom = chrom.replace("chr", "")
    if chrom == 'X':
        return 23
    elif chrom == 'Y':
        return 24
    elif chrom in ['M', 'MT']:
        return 25
    else:
        return int(chrom)

@app.callback(
    [Output("table-output", "data"),
     Output("chrom-dropdown", "options"),
     Output("chrom-dropdown", "value")],
    [Input("submit-btn", "n_clicks"),
     Input("position-input", "n_submit"),
     Input("genotype-input", "n_submit")],
    [dash.dependencies.State("chrom-dropdown", "value"),
     dash.dependencies.State("position-input", "value"),
     dash.dependencies.State("genotype-input", "value")]
)
def update_table(n_clicks, pos_submit, gen_submit, chrom, position, genotype):
    # Fetch unique chromosomes for dropdown
    options = []
    try:
        with engine.connect() as connection:
            stmt = text('SELECT DISTINCT "CHROM" FROM alpha_missense_data')
            chroms = connection.execute(stmt).fetchall()
            options = sorted([{'label': chrom[0], 'value': chrom[0]} for chrom in chroms], key=lambda x: chromosomal_order(x['value']))
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
        columns = ["CHROM", "POS", "REF", "ALT", "genome", "am_pathogenicity", "am_class", "mean_am_pathogenicity", "gene_id", "gene_name", "transcript_name", "uniprot_id", "transcript_id", "protein_variant"]
        data_dicts = [dict(zip(columns, row)) for row in data]
    return data_dicts, options, chrom

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))
    app.run_server(debug=False, host='0.0.0.0', port=port)
