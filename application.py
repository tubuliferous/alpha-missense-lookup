# Import necessary libraries
import os
import requests
import dash
from dash import dcc, html, Input, Output
import pandas as pd
import dash_table
import dash_bootstrap_components as dbc
import threading

filename = "AlphaMissense_hg38.tsv.gz"
url = "https://zenodo.org/record/8208688/files/AlphaMissense_hg38.tsv.gz?download=1"
data_ready_event = threading.Event()

def download_and_load_data():
    global df
    
    print("Downloading AlphaMissense table...")

    if not os.path.exists(filename):
        response = requests.get(url, stream=True)
        with open(filename, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)

        if os.path.exists(filename):
            print(f"{filename} has been downloaded.")
        else:
            print(f"Failed to download {filename}.")
    else:
        print(f"{filename} already exists.")
    
    print("Current Working Directory:", os.getcwd())
    print("Files in the Current Directory:", os.listdir(os.getcwd()))
    print("Loading AlphaMissense table...")
    df = pd.read_csv(filename, sep="\t", compression='gzip', skiprows=3).rename(columns={"#CHROM": "CHROM"})
    print("AlphaMissense table loaded.")
    
    data_ready_event.set()

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
    data_ready_event.wait()
    options = sorted(
                [{'label': chrom, 'value': chrom} for chrom in df['CHROM'].unique()],
                key=lambda x: (int(x['label'][3:]) if x['label'][3:].isdigit() else float('inf'), x['label'])
            )
    initial_value = df['CHROM'].unique()[0] if not chrom else chrom

    if not genotype:
        return [], options, initial_value

    genotype = genotype.upper()

    if len(genotype) == 1:
        filtered_df = df[(df['CHROM'] == chrom) & (df['POS'] == position) & 
                         (df['ALT'] == genotype)]
    else:
        filtered_df = df[(df['CHROM'] == chrom) & (df['POS'] == position) & 
                         ((df['ALT'] == genotype[0]) | (df['ALT'] == genotype[1]))]

    return filtered_df.to_dict('records'), options, initial_value

if __name__ == "__main__":
    app.run_server(debug=True)
