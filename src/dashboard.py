import dash
from dash import html, dcc
from dash.dependencies import Input, Output, State
import plotly.graph_objs as go
import pandas as pd
from pymongo import MongoClient
import datetime
import plotly.graph_objs as go

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client['anomaly_detection']
collection = db['anomaly_results']
feedback_collection = db['operator_feedback']

app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("Real-Time Anomaly Detection Dashboard"),

    dcc.Graph(id='live-graph'),

    dcc.Interval(
        id='interval-component',
        interval=5*1000,  # every 5 seconds
        n_intervals=0
    ),

    html.Div([
        html.Button("Valid Alert", id='valid-button', n_clicks=0, style={'marginRight': '10px', 'fontSize': '18px'}),
        html.Button("False Alert", id='false-button', n_clicks=0, style={'fontSize': '18px'}),
    ], style={'marginTop': '20px'}),

    html.Div(id='feedback-output', style={'marginTop': '20px', 'fontWeight': 'bold', 'color': 'green'})
])

@app.callback(
    Output('live-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)


def update_graph_live(n):
    data = list(collection.find())
    if not data:
        return go.Figure().update_layout(title='No data available')

    df = pd.DataFrame(data)
    if pd.api.types.is_numeric_dtype(df['timestamp']):
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
    else:
        df['timestamp'] = pd.to_datetime(df['timestamp'])

    normal_df = df[df['is_anomaly'] != True]
    anomaly_df = df[df['is_anomaly'] == True]

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=normal_df['timestamp'],
        y=normal_df['temperature'],
        mode='lines+markers',
        name='Normal',
        marker=dict(color='blue')
    ))

    fig.add_trace(go.Scatter(
        x=anomaly_df['timestamp'],
        y=anomaly_df['temperature'],
        mode='markers',
        name='Anomalies',
        marker=dict(color='red', size=12, symbol='circle-open')
    ))

    fig.update_layout(
        title='Temperature Sensor Readings with Anomalies',
        xaxis_title='Time',
        yaxis_title='Temperature',
        template='plotly_white',
        height=600
    )
    return fig


@app.callback(
    Output('feedback-output', 'children'),
    [Input('valid-button', 'n_clicks'),
     Input('false-button', 'n_clicks')],
    State('interval-component', 'n_intervals')
)
def handle_feedback(valid_clicks, false_clicks, n_intervals):
    ctx = dash.callback_context
    if not ctx.triggered:
        return ""
    button_id = ctx.triggered[0]['prop_id'].split('.')

    # Save feedback to MongoDB with timestamp and interval info
    feedback = {
        'timestamp': datetime.datetime.now(),
        'feedback_type': None,
        'interval': n_intervals
    }
    if button_id == 'valid-button':
        feedback['feedback_type'] = 'valid'
    elif button_id == 'false-button':
        feedback['feedback_type'] = 'false'

    feedback_collection.insert_one(feedback)

    return f"Thank you for marking this alert as '{feedback['feedback_type']}'."

if __name__ == '__main__':
    app.run(debug=True)
