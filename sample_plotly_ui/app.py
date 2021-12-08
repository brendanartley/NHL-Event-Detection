# Run this app with `python app.py` and
# visit http://127.0.0.1:8050/ in your web browser.

import dash
from dash import dcc
from dash import html
import plotly.express as px
from dash.dependencies import Input, Output

import pandas as pd

# NEED THESE TWO LINES TO BE ABLE TO BYPASS URLLIB ERROR
# import ssl
# ssl._create_default_https_context = ssl._create_unverified_context

df = pd.read_csv("sample_gt_comments/part-00000-3bf5e0da-97a4-48be-bac1-bd2320e1dfdb-c000.csv", header=None, names=["Time (UTC)"])
df['Time (UTC)'] = pd.to_datetime(df['Time (UTC)'], unit='s')
df['Count'] = 1
df = df.groupby(pd.Grouper(key='Time (UTC)', freq='1min')).count().iloc[:240, :].reset_index()

app = dash.Dash(__name__)

fig = px.line(df, x="Time (UTC)", y="Count", template="simple_white")

app.layout = html.Div(children=[
    html.H1(children='Reddit Data Sample'),

    html.Div(children='''
        Using Dash-Plotly to graph the activity of reddit users in Game Thread.
    '''),

    dcc.Graph(
        id='example-graph',
        figure=fig
    )
])

if __name__ == '__main__':
    app.run_server(debug=True)