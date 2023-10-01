import os
import requests
import dash
from dash import dcc, html, Input, Output, dash_table
import pandas as pd
import dash_bootstrap_components as dbc
import threading

# Global Variables and Data Loading
filename = "AlphaMissense_hg38.tsv.gz"
url = "https://zenodo.org/record/8208688/files/AlphaMissense_hg38.tsv.gz?download=1"
data_ready_event = threading.Event()

# Function to download and load the data in a separate thread
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
    
    print("Loading AlphaMissense table...")
    df = pd.read_csv(filename, sep="\t", compression='gzip', skiprows=3).rename(columns={"#CHROM": "CHROM"})
    print("AlphaMissense table loaded.")
    data_ready_event.set()

# Start the data loading thread
threading.Thread(target=download_and_load_data).start()

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the primary layout
app.layout = html.Div([
    html.Div(id='table-container'),
    dcc.Interval(id='data-check-interval', interval=2*1000, max_intervals=30)  # check every 2 seconds for 30 times
])

# Callback to update the layout based on data availability
@app.callback(
    Output('table-container', 'children'),
    [Input('data-check-interval', 'n_intervals')]
)
def update_layout(n):
    if data_ready_event.is_set():
        return [
            dbc.Container([
                dbc.Row(dbc.Col(html.H1("AlphaMissense Lookup"), width={"size": 6, "offset": 3}), className="mb-4"),
                dbc.Row([
                    # Dropdown, Input, and Button components go here
                    # ... 
                ]),
                dbc.Row([
                    dbc.Col(
                        dcc.Loading(
                            id="loading",
                            type="default",
                            children=[
                                dash_table.DataTable(id="table-output", columns=[{"name": col, "id": col} for col in df.columns])
                            ]
                        )
                    )
                ])
            ])
        ]
    elif n >= 30:
        return html.Div("Failed to load data within time limit.")
    else:
        return html.H1('Loading AlphaMissense data... Please wait.')

# Callback to update table (if you have other callbacks, add them as needed)
@app.callback(
    Output("table-output", "data"),
    [Input("submit-btn", "n_clicks")],
    [dash.dependencies.State("chrom-dropdown", "value"),
     dash.dependencies.State("position-input", "value"),
     dash.dependencies.State("genotype-input", "value")]
)
def update_table(n_clicks, chrom, position, genotype):
    if not genotype or not data_ready_event.is_set():
        return []

    genotype = genotype.upper()
    if len(genotype) == 1:
        filtered_df = df[(df['CHROM'] == chrom) & (df['POS'] == position) & 
                         (df['ALT'] == genotype)]
    else:
        filtered_df = df[(df['CHROM'] == chrom) & (df['POS'] == position) & 
                         ((df['ALT'] == genotype[0]) | (df['ALT'] == genotype[1]))]
    return filtered_df.to_dict('records')


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))  # 8050 is a default port you choose for local development
    app.run_server(debug=False, host='0.0.0.0', port=port)
