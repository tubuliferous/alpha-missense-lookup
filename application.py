# Import necessary libraries
import os
import requests
import dash
from dash import dcc, html, Input, Output
import pandas as pd
import dash_table
import dash_bootstrap_components as dbc

filename = "AlphaMissense_hg38.tsv.gz"
url = "https://zenodo.org/record/8208688/files/AlphaMissense_hg38.tsv.gz?download=1"

print("Downloading AlphaMissense table...")
# Check if the file already exists
if not os.path.exists(filename):
    response = requests.get(url, stream=True)
    with open(filename, "wb") as file:
        for chunk in response.iter_content(chunk_size=1024):
            if chunk:
                file.write(chunk)

    # Check to confirm the file was downloaded successfully
    if os.path.exists(filename):
        print(f"{filename} has been downloaded.")
    else:
        print(f"Failed to download {filename}.")
else:
    print(f"{filename} already exists.")


# Print the current working directory
print("Current Working Directory:", os.getcwd())

# Print the list of files in the current directory
print("Files in the Current Directory:", os.listdir(os.getcwd()))


df = pd.read_csv("AlphaMissense_hg38.tsv.gz", sep="\t", compression='gzip', skiprows=3).rename(columns={"#CHROM": "CHROM"})


# Create a Dash application
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])

# Define the app layout
app.layout = dbc.Container([
    dbc.Row(dbc.Col(html.H1("AlphaMissense Lookup"), width={"size": 6, "offset": 3}), className="mb-4"),

    dbc.Row([
        # Label and Dropdown for Chromosome
        dbc.Col([
            dbc.Label('Chromosome:'),
            dbc.Select(
                id="chrom-dropdown",
                options=sorted(
                    [{'label': chrom, 'value': chrom} for chrom in df['CHROM'].unique()],
                    key=lambda x: (int(x['label'][3:]) if x['label'][3:].isdigit() else float('inf'), x['label'])
                ),
                value=df['CHROM'].unique()[0]
            )
        ], width=2),

        # Label and Input for Position
        dbc.Col([
            dbc.Label('Position:'),
            dbc.Input(id="position-input", type="number", placeholder="Enter position")
        ], width=2),

        # Label and Input for Genotype
        dbc.Col([
            dbc.Label('Genotype:'),
            dbc.Input(id="genotype-input", type="text", placeholder="Enter genotype")
        ], width=2),

        # Submit Button
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
                    dash_table.DataTable(id="table-output", columns=[{"name": col, "id": col} for col in df.columns])
                ]
            )
        )
    ])
], fluid=True)


# To make the input fields submit when Enter is pressed, we can bind a script.
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



# Define callback to update the table based on user input
@app.callback(
    Output("table-output", "data"),
    [Input("submit-btn", "n_clicks")],
    [dash.dependencies.State("chrom-dropdown", "value"),
     dash.dependencies.State("position-input", "value"),
     dash.dependencies.State("genotype-input", "value")]
)
def update_table(n_clicks, chrom, position, genotype):
    print("Callback executed!")  # Check if callback is triggered

    if not genotype:
        return []

    # Convert genotype to uppercase
    genotype = genotype.upper()

    # Print inputs to check them
    # print(f"Chrom: {chrom}, Position: {position}, Genotype: {genotype}")

    if len(genotype) == 1:
        filtered_df = df[(df['CHROM'] == chrom) & (df['POS'] == position) & 
                         (df['ALT'] == genotype)]
    else:
        filtered_df = df[(df['CHROM'] == chrom) & (df['POS'] == position) & 
                         ((df['ALT'] == genotype[0]) | (df['ALT'] == genotype[1]))]

    # Print the filtered data
    print(filtered_df)

    return filtered_df.to_dict('records')

# Run the app
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8050))  # 8050 is a default port you choose for local development
    app.run_server(debug=False, host='0.0.0.0', port=port)
