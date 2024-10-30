# src/user_activity_dashboard.py
import os
import dash
from dash import dcc, html, Input, Output
import plotly.graph_objs as go
import psycopg2
import pandas as pd
from confluent_kafka import Consumer
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Redshift connection details
redshift_host = os.getenv("REDSHIFT_HOST")
redshift_db = os.getenv("REDSHIFT_DB")
redshift_user = os.getenv("REDSHIFT_USER")
redshift_password = os.getenv("REDSHIFT_PASSWORD")
redshift_port = os.getenv("REDSHIFT_PORT", 5439)

# Kafka setup
kafka_bootstrap_servers = 'localhost:9092'
topic = "user-events"

# Initialize Dash app
app = dash.Dash(__name__)

# Layout for the dashboard
app.layout = html.Div(children=[
    html.H1("User Activity Dashboard"),
    
    # Real-time event counts
    html.Div([
        html.H3("Event Type Counts"),
        dcc.Graph(id='event-counts-graph')
    ]),
    
    # Average session duration
    html.Div([
        html.H3("Average Session Duration"),
        dcc.Graph(id='session-duration-graph')
    ]),
    
    # Error rate
    html.Div([
        html.H3("Error Rate"),
        dcc.Graph(id='error-rate-graph')
    ]),
    
    # Kafka Lag (Real-Time)
    html.Div([
        html.H3("Kafka Consumer Lag"),
        dcc.Graph(id='kafka-lag-graph')
    ]),

    dcc.Interval(
        id='interval-component',
        interval=60*1000,  # Update every minute
        n_intervals=0
    )
])

# Helper function to fetch data from Redshift
def fetch_data_from_redshift(query):
    conn = psycopg2.connect(
        dbname=redshift_db,
        user=redshift_user,
        password=redshift_password,
        host=redshift_host,
        port=redshift_port
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Callback for event type counts
@app.callback(
    Output('event-counts-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_event_counts(n):
    query = "SELECT event_type, COUNT(*) as count FROM user_events GROUP BY event_type;"
    df = fetch_data_from_redshift(query)
    return {
        'data': [go.Bar(x=df['event_type'], y=df['count'])],
        'layout': go.Layout(title="Event Counts by Type")
    }

# Callback for session duration
@app.callback(
    Output('session-duration-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_session_duration(n):
    query = """
    SELECT user_id, AVG(EXTRACT(epoch FROM timestamp)) AS avg_duration
    FROM user_events
    GROUP BY user_id;
    """
    df = fetch_data_from_redshift(query)
    return {
        'data': [go.Scatter(x=df['user_id'], y=df['avg_duration'], mode='markers')],
        'layout': go.Layout(title="Average Session Duration")
    }

# Callback for error rate
@app.callback(
    Output('error-rate-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_error_rate(n):
    query = "SELECT COUNT(*) AS errors FROM user_events WHERE event_type = 'error';"
    df = fetch_data_from_redshift(query)
    return {
        'data': [go.Indicator(mode="gauge+number", value=df['errors'].iloc[0], title="Error Rate")],
        'layout': go.Layout(title="Error Rate")
    }

# Callback for Kafka Lag
@app.callback(
    Output('kafka-lag-graph', 'figure'),
    Input('interval-component', 'n_intervals')
)
def update_kafka_lag(n):
    consumer = Consumer({
        'bootstrap.servers': kafka_bootstrap_servers,
        'group.id': 'event-consumer-group',
        'auto.offset.reset': 'earliest'
    })
    consumer.subscribe([topic])

    # Fetch lag for each partition
    lag_values = []
    partitions = consumer.assignment()
    for partition in partitions:
        committed_offset = consumer.committed(partition).offset
        high_offset = consumer.get_watermark_offsets(partition)[1]
        lag = high_offset - committed_offset
        lag_values.append(lag)

    consumer.close()
    total_lag = sum(lag_values)
    
    return {
        'data': [go.Indicator(mode="gauge+number", value=total_lag, title="Kafka Lag")],
        'layout': go.Layout(title="Kafka Consumer Lag")
    }

if __name__ == '__main__':
    app.run_server(debug=True)
