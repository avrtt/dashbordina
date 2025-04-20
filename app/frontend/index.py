import dash
from dash import dcc, html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State, ClientsideFunction
import plotly.express as px
import plotly.graph_objects as go
from flask_caching import Cache
import pandas as pd
import requests
from datetime import datetime, timedelta
import json
import os

# Initialize the Dash app with dark theme
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.DARKLY],
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
    suppress_callback_exceptions=True
)

server = app.server

# Setup caching
cache = Cache(
    app.server,
    config={
        'CACHE_TYPE': 'filesystem',
        'CACHE_DIR': 'cache-directory',
        'CACHE_DEFAULT_TIMEOUT': 300  # 5 minutes
    }
)

# API base URL from environment variable or default to mock API
API_BASE_URL = os.environ.get('API_BASE_URL', 'http://mock-api:8000')

# Cache the API responses
@cache.memoize(timeout=300)
def get_api_data(endpoint, params=None):
    """Get data from API with caching"""
    try:
        response = requests.get(f"{API_BASE_URL}/{endpoint}", params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching data from API: {e}")
        return {}

# Helper functions to get data
def get_channels():
    return get_api_data("channels")

def get_segments():
    return get_api_data("segments")

def get_metrics(start_date=None, end_date=None, channel_id=None, segment_id=None):
    params = {}
    if start_date:
        params['start_date'] = start_date
    if end_date:
        params['end_date'] = end_date
    if channel_id:
        params['channel_id'] = channel_id
    if segment_id:
        params['segment_id'] = segment_id
    
    return get_api_data("metrics", params)

# Dark theme colors
colors = {
    'background': '#121212',
    'card': '#1e1e1e',
    'text': '#ffffff',
    'accent1': '#1976D2',  # Blue
    'accent2': '#2E7D32',  # Green
    'accent3': '#C62828',  # Red
    'accent4': '#F9A825',  # Yellow
    'grid': '#333333'
}

# Define the layout
app.layout = html.Div(
    style={'backgroundColor': colors['background'], 'color': colors['text'], 'minHeight': '100vh', 'padding': '1rem'},
    children=[
        # Store for client-side caching
        dcc.Store(id='metrics-data'),
        
        # Header
        html.Div([
            html.H1("Marketing Analytics Dashboard", style={'textAlign': 'center', 'marginBottom': '2rem'}),
            
            # Date range picker and filters row
            dbc.Row([
                # Date picker column
                dbc.Col([
                    html.Label("Date Range"),
                    dcc.DatePickerRange(
                        id='date-picker-range',
                        start_date=(datetime.now() - timedelta(days=30)).date(),
                        end_date=datetime.now().date(),
                        display_format='YYYY-MM-DD',
                        style={'color': '#121212'},
                    )
                ], width=4),
                
                # Channel dropdown column
                dbc.Col([
                    html.Label("Channel"),
                    dcc.Dropdown(
                        id='channel-dropdown',
                        options=[{'label': 'All Channels', 'value': 'all'}],
                        value='all',
                        clearable=False,
                        style={
                            'backgroundColor': colors['card'],
                            'color': colors['text']
                        }
                    )
                ], width=4),
                
                # Segment dropdown column
                dbc.Col([
                    html.Label("Segment"),
                    dcc.Dropdown(
                        id='segment-dropdown',
                        options=[{'label': 'All Segments', 'value': 'all'}],
                        value='all',
                        clearable=False,
                        style={
                            'backgroundColor': colors['card'],
                            'color': colors['text']
                        }
                    )
                ], width=4)
            ], className='mb-4'),
            
            # Top metrics cards row
            dbc.Row([
                # CAC card
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Customer Acquisition Cost", className="card-title"),
                            html.Div(id='cac-value', className="display-4 text-center"),
                            html.Div(id='cac-change', className="text-center")
                        ])
                    ], className="h-100", style={'backgroundColor': colors['card']})
                ], width=3),
                
                # CLV card
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Customer Lifetime Value", className="card-title"),
                            html.Div(id='clv-value', className="display-4 text-center"),
                            html.Div(id='clv-change', className="text-center")
                        ])
                    ], className="h-100", style={'backgroundColor': colors['card']})
                ], width=3),
                
                # ROAS card
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Return on Ad Spend", className="card-title"),
                            html.Div(id='roas-value', className="display-4 text-center"),
                            html.Div(id='roas-change', className="text-center")
                        ])
                    ], className="h-100", style={'backgroundColor': colors['card']})
                ], width=3),
                
                # Conversion Rate card
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H5("Conversion Rate", className="card-title"),
                            html.Div(id='conversion-rate-value', className="display-4 text-center"),
                            html.Div(id='conversion-rate-change', className="text-center")
                        ])
                    ], className="h-100", style={'backgroundColor': colors['card']})
                ], width=3)
            ], className='mb-4'),
            
            # Main charts row
            dbc.Row([
                # Channel Performance
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Channel Performance"),
                        dbc.CardBody([
                            dcc.Graph(
                                id='channel-performance-chart',
                                config={'displayModeBar': False},
                                figure=go.Figure(),
                                style={'height': '400px'}
                            )
                        ])
                    ], style={'backgroundColor': colors['card']})
                ], width=6),
                
                # Segment Performance
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Segment Performance"),
                        dbc.CardBody([
                            dcc.Graph(
                                id='segment-performance-chart',
                                config={'displayModeBar': False},
                                figure=go.Figure(),
                                style={'height': '400px'}
                            )
                        ])
                    ], style={'backgroundColor': colors['card']})
                ], width=6)
            ], className='mb-4'),
            
            # Secondary charts row
            dbc.Row([
                # Campaign Performance
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader("Campaign Performance"),
                        dbc.CardBody([
                            dcc.Graph(
                                id='campaign-performance-chart',
                                config={'displayModeBar': False},
                                figure=go.Figure(),
                                style={'height': '400px'}
                            )
                        ])
                    ], style={'backgroundColor': colors['card']})
                ], width=12)
            ]),
            
            # Campaign details row (initially hidden, shown on drill-down)
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardHeader(id="campaign-detail-header", children="Campaign Details"),
                        dbc.CardBody([
                            dcc.Graph(
                                id='campaign-detail-chart',
                                config={'displayModeBar': False},
                                figure=go.Figure(),
                                style={'height': '400px'}
                            )
                        ])
                    ], style={'backgroundColor': colors['card'], 'display': 'none'}, id='campaign-detail-card')
                ], width=12)
            ], className='mb-4'),
            
            # Tooltips for metrics
            html.Div([
                dbc.Tooltip(
                    "Customer Acquisition Cost: Total marketing spend divided by new customers acquired",
                    target="cac-value",
                ),
                dbc.Tooltip(
                    "Customer Lifetime Value: Predicted total revenue from a customer over their relationship with the business",
                    target="clv-value",
                ),
                dbc.Tooltip(
                    "Return on Ad Spend: Revenue attributed to advertising divided by the advertising cost",
                    target="roas-value",
                ),
                dbc.Tooltip(
                    "Conversion Rate: Percentage of visitors who take a desired action",
                    target="conversion-rate-value",
                ),
            ]),
        ])
    ]
)

# Populate dropdowns on page load
@app.callback(
    [Output('channel-dropdown', 'options'),
     Output('segment-dropdown', 'options')],
    [Input('date-picker-range', 'start_date')]
)
def populate_dropdowns(start_date):
    # Get channels from API
    channels_data = get_channels()
    channel_options = [{'label': 'All Channels', 'value': 'all'}]
    for channel in channels_data:
        channel_options.append({
            'label': channel['channel_name'],
            'value': channel['channel_id']
        })
    
    # Get segments from API
    segments_data = get_segments()
    segment_options = [{'label': 'All Segments', 'value': 'all'}]
    for segment in segments_data:
        segment_options.append({
            'label': segment['segment_name'],
            'value': segment['segment_id']
        })
    
    return channel_options, segment_options

# Fetch metrics data and store in client-side cache
@app.callback(
    Output('metrics-data', 'data'),
    [Input('date-picker-range', 'start_date'),
     Input('date-picker-range', 'end_date'),
     Input('channel-dropdown', 'value'),
     Input('segment-dropdown', 'value')]
)
def fetch_metrics_data(start_date, end_date, channel_id, segment_id):
    params = {
        'start_date': start_date,
        'end_date': end_date
    }
    
    if channel_id and channel_id != 'all':
        params['channel_id'] = channel_id
    
    if segment_id and segment_id != 'all':
        params['segment_id'] = segment_id
    
    metrics_data = get_metrics(**params)
    return metrics_data

# Update KPI metrics
@app.callback(
    [Output('cac-value', 'children'),
     Output('clv-value', 'children'),
     Output('roas-value', 'children'),
     Output('conversion-rate-value', 'children')],
    [Input('metrics-data', 'data')]
)
def update_kpi_metrics(data):
    if not data or not data.get('segment_cac') or not data.get('channel_roas'):
        return "$0.00", "$0.00", "0.0x", "0.0%"
    
    # Calculate average CAC
    cac_values = [item['cac'] for item in data['segment_cac'] if item['cac'] is not None]
    avg_cac = sum(cac_values) / len(cac_values) if cac_values else 0
    
    # Calculate CLV (simplified as 3x the average conversion value)
    avg_conversion_values = [item['avg_conversion_value'] for item in data['segment_performance'] if item['avg_conversion_value'] is not None]
    avg_clv = 3 * (sum(avg_conversion_values) / len(avg_conversion_values) if avg_conversion_values else 0)
    
    # Calculate average ROAS
    roas_values = [item['roas'] for item in data['channel_roas'] if item['roas'] is not None]
    avg_roas = sum(roas_values) / len(roas_values) if roas_values else 0
    
    # Calculate conversion rate
    if data.get('channel_performance'):
        total_clicks = sum(item['clicks'] for item in data['channel_performance'] if item['clicks'] is not None)
        total_conversions = sum(item['conversions'] for item in data['campaign_performance'] if item['conversions'] is not None)
        conversion_rate = (total_conversions / total_clicks) * 100 if total_clicks > 0 else 0
    else:
        conversion_rate = 0
    
    return f"${avg_cac:.2f}", f"${avg_clv:.2f}", f"{avg_roas:.1f}x", f"{conversion_rate:.1f}%"

# Update Channel Performance Chart
@app.callback(
    Output('channel-performance-chart', 'figure'),
    [Input('metrics-data', 'data')]
)
def update_channel_performance(data):
    if not data or not data.get('channel_performance'):
        # Return empty figure with styled axes
        fig = go.Figure()
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font={'color': colors['text']},
            margin=dict(l=40, r=40, t=40, b=40),
            xaxis={'gridcolor': colors['grid']},
            yaxis={'gridcolor': colors['grid']}
        )
        return fig
    
    # Prepare data for the chart
    channel_data = data['channel_performance']
    
    # Group by channel and calculate total clicks and impressions
    channels = {}
    for item in channel_data:
        channel_name = item['channel_name']
        if channel_name not in channels:
            channels[channel_name] = {'clicks': 0, 'impressions': 0}
        
        channels[channel_name]['clicks'] += item['clicks'] if item['clicks'] is not None else 0
        channels[channel_name]['impressions'] += item['impressions'] if item['impressions'] is not None else 0
    
    # Calculate CTR and create dataframe
    channel_names = []
    ctrs = []
    clicks = []
    impressions = []
    
    for name, data in channels.items():
        channel_names.append(name)
        clicks.append(data['clicks'])
        impressions.append(data['impressions'])
        ctrs.append((data['clicks'] / data['impressions'] * 100) if data['impressions'] > 0 else 0)
    
    # Create figure
    fig = go.Figure()
    
    # Add bars for clicks
    fig.add_trace(go.Bar(
        x=channel_names,
        y=clicks,
        name='Clicks',
        marker_color=colors['accent1'],
        hovertemplate='Clicks: %{y}<extra></extra>'
    ))
    
    # Add line for CTR
    fig.add_trace(go.Scatter(
        x=channel_names,
        y=ctrs,
        name='CTR (%)',
        mode='lines+markers',
        yaxis='y2',
        line=dict(color=colors['accent4'], width=3),
        marker=dict(size=8),
        hovertemplate='CTR: %{y:.2f}%<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        barmode='group',
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font={'color': colors['text']},
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis={'gridcolor': colors['grid']},
        yaxis={
            'title': 'Clicks',
            'gridcolor': colors['grid']
        },
        yaxis2={
            'title': 'CTR (%)',
            'overlaying': 'y',
            'side': 'right',
            'gridcolor': colors['grid']
        },
        hovermode='x unified'
    )
    
    return fig

# Update Segment Performance Chart
@app.callback(
    Output('segment-performance-chart', 'figure'),
    [Input('metrics-data', 'data')]
)
def update_segment_performance(data):
    if not data or not data.get('segment_performance') or not data.get('segment_cac'):
        # Return empty figure with styled axes
        fig = go.Figure()
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font={'color': colors['text']},
            margin=dict(l=40, r=40, t=40, b=40),
            xaxis={'gridcolor': colors['grid']},
            yaxis={'gridcolor': colors['grid']}
        )
        return fig
    
    # Prepare data for the chart
    segment_performance = data['segment_performance']
    segment_cac = data['segment_cac']
    
    # Group by segment and calculate total conversions and values
    segments = {}
    for item in segment_performance:
        segment_name = item['segment_name']
        if segment_name not in segments:
            segments[segment_name] = {
                'conversions': 0, 
                'total_value': 0,
                'segment_id': item['segment_id']
            }
        
        segments[segment_name]['conversions'] += item['conversions'] if item['conversions'] is not None else 0
        segments[segment_name]['total_value'] += item['total_conversion_value'] if item['total_conversion_value'] is not None else 0
    
    # Add CAC to each segment
    for item in segment_cac:
        segment_name = item['segment_name']
        if segment_name in segments:
            if 'cac' not in segments[segment_name]:
                segments[segment_name]['cac'] = 0
                segments[segment_name]['cac_count'] = 0
            
            if item['cac'] is not None:
                segments[segment_name]['cac'] += item['cac']
                segments[segment_name]['cac_count'] += 1
    
    # Calculate average CAC
    for segment in segments.values():
        if 'cac_count' in segment and segment['cac_count'] > 0:
            segment['avg_cac'] = segment['cac'] / segment['cac_count']
        else:
            segment['avg_cac'] = 0
    
    # Create lists for plotting
    segment_names = list(segments.keys())
    conversions = [segments[name]['conversions'] for name in segment_names]
    avg_cacs = [segments[name]['avg_cac'] for name in segment_names]
    
    # Create figure
    fig = go.Figure()
    
    # Add bars for conversions
    fig.add_trace(go.Bar(
        x=segment_names,
        y=conversions,
        name='Conversions',
        marker_color=colors['accent2'],
        customdata=[segments[name]['segment_id'] for name in segment_names],
        hovertemplate='Conversions: %{y}<extra></extra>'
    ))
    
    # Add line for CAC
    fig.add_trace(go.Scatter(
        x=segment_names,
        y=avg_cacs,
        name='Avg. CAC ($)',
        mode='lines+markers',
        yaxis='y2',
        line=dict(color=colors['accent3'], width=3),
        marker=dict(size=8),
        hovertemplate='CAC: $%{y:.2f}<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font={'color': colors['text']},
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis={'gridcolor': colors['grid']},
        yaxis={
            'title': 'Conversions',
            'gridcolor': colors['grid']
        },
        yaxis2={
            'title': 'CAC ($)',
            'overlaying': 'y',
            'side': 'right',
            'gridcolor': colors['grid']
        },
        hovermode='x unified'
    )
    
    return fig

# Update Campaign Performance Chart
@app.callback(
    Output('campaign-performance-chart', 'figure'),
    [Input('metrics-data', 'data')]
)
def update_campaign_performance(data):
    if not data or not data.get('campaign_performance'):
        # Return empty figure with styled axes
        fig = go.Figure()
        fig.update_layout(
            plot_bgcolor=colors['background'],
            paper_bgcolor=colors['background'],
            font={'color': colors['text']},
            margin=dict(l=40, r=40, t=40, b=40),
            xaxis={'gridcolor': colors['grid']},
            yaxis={'gridcolor': colors['grid']}
        )
        return fig
    
    # Prepare data for the chart
    campaign_data = data['campaign_performance']
    
    # Group by campaign
    campaigns = {}
    for item in campaign_data:
        campaign_name = item['campaign_name']
        if campaign_name not in campaigns:
            campaigns[campaign_name] = {
                'conversions': 0,
                'total_value': 0,
                'campaign_id': item['campaign_id'],
                'channel_name': item['channel_name']
            }
        
        campaigns[campaign_name]['conversions'] += item['conversions'] if item['conversions'] is not None else 0
        campaigns[campaign_name]['total_value'] += item['total_conversion_value'] if item['total_conversion_value'] is not None else 0
    
    # Sort by total value and get top campaigns
    sorted_campaigns = sorted(campaigns.items(), key=lambda x: x[1]['total_value'], reverse=True)
    
    # Create lists for plotting
    campaign_names = [c[0] for c in sorted_campaigns]
    campaign_ids = [c[1]['campaign_id'] for c in sorted_campaigns]
    channels = [c[1]['channel_name'] for c in sorted_campaigns]
    conversions = [c[1]['conversions'] for c in sorted_campaigns]
    values = [c[1]['total_value'] for c in sorted_campaigns]
    
    # Create colored bars by channel
    channel_names = list(set(channels))
    channel_colors = {
        channel: colors[f'accent{i % 4 + 1}'] 
        for i, channel in enumerate(channel_names)
    }
    
    # Create figure
    fig = go.Figure()
    
    # Add bars for campaign values
    for i, campaign in enumerate(campaign_names):
        fig.add_trace(go.Bar(
            x=[campaign],
            y=[values[i]],
            name=channels[i],
            marker_color=channel_colors[channels[i]],
            customdata=[[campaign_ids[i], conversions[i]]],
            hovertemplate=(
                'Campaign: %{x}<br>' +
                'Channel: ' + channels[i] + '<br>' +
                'Value: $%{y:.2f}<br>' +
                'Conversions: %{customdata[1]}<extra></extra>'
            )
        ))
    
    # Update layout
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font={'color': colors['text']},
        margin=dict(l=40, r=40, t=40, b=60),
        legend=dict(
            title="Channels",
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis={
            'title': 'Campaign',
            'gridcolor': colors['grid'],
            'categoryorder': 'total descending'
        },
        yaxis={
            'title': 'Conversion Value ($)',
            'gridcolor': colors['grid']
        },
        hovermode='closest'
    )
    
    return fig

# Show campaign details on click
@app.callback(
    [Output('campaign-detail-card', 'style'),
     Output('campaign-detail-header', 'children'),
     Output('campaign-detail-chart', 'figure')],
    [Input('campaign-performance-chart', 'clickData')],
    [State('metrics-data', 'data')]
)
def display_campaign_details(clickData, data):
    if not clickData or not data:
        return {'display': 'none'}, "Campaign Details", go.Figure()
    
    # Get campaign ID and name from click data
    campaign_id = clickData['points'][0]['customdata'][0]
    campaign_name = clickData['points'][0]['x']
    
    # Filter data for this campaign
    campaign_data = [item for item in data['campaign_performance'] if item['campaign_id'] == campaign_id]
    
    if not campaign_data:
        return {'display': 'none'}, "Campaign Details", go.Figure()
    
    # Sort by date
    campaign_data = sorted(campaign_data, key=lambda x: x['date'])
    
    # Extract data for plotting
    dates = [item['date'] for item in campaign_data]
    values = [item['total_conversion_value'] if item['total_conversion_value'] is not None else 0 for item in campaign_data]
    conversions = [item['conversions'] if item['conversions'] is not None else 0 for item in campaign_data]
    
    # Create figure
    fig = go.Figure()
    
    # Add bars for daily values
    fig.add_trace(go.Bar(
        x=dates,
        y=values,
        name='Conversion Value',
        marker_color=colors['accent1'],
        hovertemplate='Date: %{x}<br>Value: $%{y:.2f}<extra></extra>'
    ))
    
    # Add line for conversions
    fig.add_trace(go.Scatter(
        x=dates,
        y=conversions,
        name='Conversions',
        mode='lines+markers',
        yaxis='y2',
        line=dict(color=colors['accent2'], width=3),
        marker=dict(size=8),
        hovertemplate='Date: %{x}<br>Conversions: %{y}<extra></extra>'
    ))
    
    # Update layout
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font={'color': colors['text']},
        margin=dict(l=40, r=40, t=40, b=40),
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis={
            'title': 'Date',
            'gridcolor': colors['grid'],
            'type': 'date'
        },
        yaxis={
            'title': 'Conversion Value ($)',
            'gridcolor': colors['grid']
        },
        yaxis2={
            'title': 'Conversions',
            'overlaying': 'y',
            'side': 'right',
            'gridcolor': colors['grid']
        },
        hovermode='x unified'
    )
    
    return {'display': 'block', 'backgroundColor': colors['card']}, f"Campaign Details: {campaign_name}", fig

# Register the clientside callback for responsive updates
app.clientside_callback(
    ClientsideFunction(
        namespace='clientside',
        function_name='resize_charts'
    ),
    Output('dummy-output', 'children'),
    [Input('window-resize', 'n_intervals')]
)

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True, host='0.0.0.0', port=8050) 