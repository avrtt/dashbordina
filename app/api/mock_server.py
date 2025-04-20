#!/usr/bin/env python3
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import sqlalchemy as sa
from sqlalchemy import text
import pandas as pd
from typing import Optional, List, Dict, Any
from datetime import date, datetime, timedelta
import os

# Create FastAPI app
app = FastAPI(title="Marketing Analytics API", 
              description="API for marketing analytics dashboard")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@postgres/airflow')
engine = sa.create_engine(DATABASE_URL)

# API endpoints
@app.get("/")
def read_root():
    return {"message": "Welcome to the Marketing Analytics API"}

@app.get("/channels/")
def get_channels():
    """Get all marketing channels"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, name, type, cost_model FROM analytics.channels"))
        channels = []
        for row in result:
            channels.append({
                "channel_id": row[0],
                "channel_name": row[1],
                "channel_type": row[2],
                "cost_model": row[3]
            })
        return channels

@app.get("/segments/")
def get_segments():
    """Get all user segments"""
    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, name, description FROM analytics.segments"))
        segments = []
        for row in result:
            segments.append({
                "segment_id": row[0],
                "segment_name": row[1],
                "segment_description": row[2]
            })
        return segments

@app.get("/campaigns/")
def get_campaigns(channel_id: Optional[int] = None):
    """Get all campaigns with optional channel filter"""
    query = "SELECT id, name, channel_id, start_date, end_date, budget, spend, status FROM analytics.campaigns"
    if channel_id:
        query += f" WHERE channel_id = {channel_id}"
    
    with engine.connect() as conn:
        result = conn.execute(text(query))
        campaigns = []
        for row in result:
            campaigns.append({
                "campaign_id": row[0],
                "campaign_name": row[1],
                "channel_id": row[2],
                "start_date": row[3].isoformat() if row[3] else None,
                "end_date": row[4].isoformat() if row[4] else None,
                "budget": float(row[5]),
                "spend_to_date": float(row[6]),
                "status": row[7]
            })
        return campaigns

@app.get("/metrics/")
def get_metrics(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    channel_id: Optional[int] = None,
    segment_id: Optional[int] = None
):
    """Get all metrics with optional filters"""
    # Set default dates if not provided
    if not start_date:
        start_date = datetime.now().date() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now().date()
    
    metrics = {}
    
    # Fetch campaign performance
    with engine.connect() as conn:
        campaign_query = """
        SELECT 
            date, campaign_id, campaign_name, channel_id, channel_name, 
            conversions, total_conversion_value, avg_conversion_value
        FROM analytics.daily_campaign_performance
        WHERE date BETWEEN :start_date AND :end_date
        """
        if channel_id:
            campaign_query += f" AND channel_id = {channel_id}"
        
        campaign_result = pd.read_sql(
            campaign_query, 
            conn, 
            params={"start_date": start_date, "end_date": end_date}
        )
        
        if not campaign_result.empty:
            metrics["campaign_performance"] = campaign_result.to_dict(orient="records")
        else:
            metrics["campaign_performance"] = []
        
        # Fetch channel performance
        channel_query = """
        SELECT 
            date, channel_id, channel_name, events, unique_users, 
            clicks, impressions, ctr
        FROM analytics.daily_channel_performance
        WHERE date BETWEEN :start_date AND :end_date
        """
        if channel_id:
            channel_query += f" AND channel_id = {channel_id}"
        
        channel_result = pd.read_sql(
            channel_query, 
            conn, 
            params={"start_date": start_date, "end_date": end_date}
        )
        
        if not channel_result.empty:
            metrics["channel_performance"] = channel_result.to_dict(orient="records")
        else:
            metrics["channel_performance"] = []
        
        # Fetch segment performance
        segment_query = """
        SELECT 
            date, segment_id, segment_name, conversions, 
            total_conversion_value, avg_conversion_value
        FROM analytics.segment_performance
        WHERE date BETWEEN :start_date AND :end_date
        """
        if segment_id:
            segment_query += f" AND segment_id = {segment_id}"
        
        segment_result = pd.read_sql(
            segment_query, 
            conn, 
            params={"start_date": start_date, "end_date": end_date}
        )
        
        if not segment_result.empty:
            metrics["segment_performance"] = segment_result.to_dict(orient="records")
        else:
            metrics["segment_performance"] = []
        
        # Fetch segment CAC
        cac_query = """
        SELECT date, segment_id, segment_name, cac
        FROM analytics.segment_cac
        WHERE date BETWEEN :start_date AND :end_date
        """
        if segment_id:
            cac_query += f" AND segment_id = {segment_id}"
        
        cac_result = pd.read_sql(
            cac_query, 
            conn, 
            params={"start_date": start_date, "end_date": end_date}
        )
        
        if not cac_result.empty:
            metrics["segment_cac"] = cac_result.to_dict(orient="records")
        else:
            metrics["segment_cac"] = []
        
        # Fetch channel ROAS
        roas_query = """
        SELECT date, channel_id, channel_name, roas
        FROM analytics.channel_roas
        WHERE date BETWEEN :start_date AND :end_date
        """
        if channel_id:
            roas_query += f" AND channel_id = {channel_id}"
        
        roas_result = pd.read_sql(
            roas_query, 
            conn, 
            params={"start_date": start_date, "end_date": end_date}
        )
        
        if not roas_result.empty:
            metrics["channel_roas"] = roas_result.to_dict(orient="records")
        else:
            metrics["channel_roas"] = []
    
    # Generate KPI summary
    metrics["kpi_summary"] = generate_kpi_summary(metrics)
    
    return metrics

def generate_kpi_summary(metrics):
    """Generate KPI summary from metrics data"""
    kpi_summary = {
        "cac": 0,
        "clv": 0,
        "roas": 0,
        "conversion_rate": 0,
        "cac_change": 0,
        "clv_change": 0,
        "roas_change": 0,
        "conversion_rate_change": 0
    }
    
    # Calculate CAC (Customer Acquisition Cost)
    if metrics.get("segment_cac"):
        cac_values = [item.get("cac", 0) for item in metrics["segment_cac"]]
        if cac_values:
            kpi_summary["cac"] = round(sum(cac_values) / len(cac_values), 2)
            kpi_summary["cac_change"] = round(random.uniform(-15, 15), 2)  # Random change for demo
    
    # Calculate CLV (Customer Lifetime Value)
    if metrics.get("campaign_performance"):
        conversion_values = [item.get("avg_conversion_value", 0) for item in metrics["campaign_performance"]]
        if conversion_values:
            avg_value = sum(conversion_values) / len(conversion_values)
            kpi_summary["clv"] = round(avg_value * 3, 2)  # Multiply by LTV multiplier
            kpi_summary["clv_change"] = round(random.uniform(-10, 20), 2)  # Random change for demo
    
    # Calculate ROAS (Return on Ad Spend)
    if metrics.get("channel_roas"):
        roas_values = [item.get("roas", 0) for item in metrics["channel_roas"]]
        if roas_values:
            kpi_summary["roas"] = round(sum(roas_values) / len(roas_values), 2)
            kpi_summary["roas_change"] = round(random.uniform(-20, 40), 2)  # Random change for demo
    
    # Calculate Conversion Rate
    if metrics.get("channel_performance") and metrics.get("campaign_performance"):
        total_clicks = sum(item.get("clicks", 0) for item in metrics["channel_performance"])
        total_conversions = sum(item.get("conversions", 0) for item in metrics["campaign_performance"])
        
        if total_clicks > 0:
            kpi_summary["conversion_rate"] = round((total_conversions / total_clicks) * 100, 2)
            kpi_summary["conversion_rate_change"] = round(random.uniform(-5, 25), 2)  # Random change for demo
    
    return kpi_summary

# Add this import at the top
import random

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 