import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd

from src.database.database import get_session
from src.database.models import Channels, VideoData, ChannelHistory, VideoHistory

from sqlmodel import select

# Map table names to SQLModel classes
TABLES = {
    "Channels": Channels,
    "VideoData": VideoData,
    "ChannelHistory": ChannelHistory,
    "VideoHistory": VideoHistory,
}

app = dash.Dash(__name__, requests_pathname_prefix='/dash/')
session = get_session()

def get_table_df(table_class, limit=100):
    rows = session.exec(select(table_class).limit(limit)).all()
    if not rows:
        return pd.DataFrame()
    # Convert SQLModel objects to dicts
    df = pd.DataFrame([row.dict() for row in rows])
    return df

app.layout = html.Div([
    html.H1("Database Explorer"),
    html.Label("Select Table"),
    dcc.Dropdown(
        id="table-dropdown",
        options=[{"label": k, "value": k} for k in TABLES.keys()],
        value="Channels"
    ),
    html.Br(),
    html.Label("Select X Axis"),
    dcc.Dropdown(id="x-dropdown"),
    html.Br(),
    html.Label("Select Y Axis"),
    dcc.Dropdown(id="y-dropdown"),
    html.Br(),
    html.Button("Update", id="update-btn"),
    html.Br(),
    html.H2("Table Data"),
    dcc.Loading(dcc.Graph(id="table-view")),
    html.H2("Plot"),
    dcc.Loading(dcc.Graph(id="plot-view")),
])

@app.callback(
    Output("x-dropdown", "options"),
    Output("y-dropdown", "options"),
    Output("x-dropdown", "value"),
    Output("y-dropdown", "value"),
    Input("table-dropdown", "value"),
)
def update_column_dropdowns(table_name):
    df = get_table_df(TABLES[table_name], limit=100)
    options = [{"label": col, "value": col} for col in df.columns]
    x_val = df.columns[0] if len(df.columns) > 0 else None
    y_val = df.columns[1] if len(df.columns) > 1 else None
    return options, options, x_val, y_val

@app.callback(
    Output("table-view", "figure"),
    Output("plot-view", "figure"),
    Input("update-btn", "n_clicks"),
    Input("table-dropdown", "value"),
    Input("x-dropdown", "value"),
    Input("y-dropdown", "value"),
)
def update_table_and_plot(n_clicks, table_name, x_col, y_col):
    df = get_table_df(TABLES[table_name])
    # Table view
    table_fig = px.scatter(df, x=df.columns[0], y=df.columns[1]) if not df.empty else {}
    table_fig = {
        "data": [],
        "layout": {
            "title": "Table Data",
            "xaxis": {"visible": False},
            "yaxis": {"visible": False},
            "annotations": [
                {
                    "text": df.to_html(index=False) if not df.empty else "No data",
                    "xref": "paper",
                    "yref": "paper",
                    "showarrow": False,
                    "font": {"size": 14}
                }
            ]
        }
    }
    # Plot view
    plot_fig = {}
    if not df.empty and x_col in df.columns and y_col in df.columns:
        plot_fig = px.scatter(df, x=x_col, y=y_col, title=f"{table_name}: {y_col} vs {x_col}")
    return table_fig, plot_fig

if __name__ == "__main__":
    app.run_server(debug=True)