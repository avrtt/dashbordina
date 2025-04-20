#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json
import random
from sqlalchemy import create_engine, text

# Database connection
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@postgres/airflow')
engine = create_engine(DATABASE_URL)

# Global settings
START_DATE = datetime.now() - timedelta(days=90)
END_DATE = datetime.now()
NUM_USERS = 50  # Reduced for testing
NUM_CHANNELS = 5
NUM_CAMPAIGNS = 15
NUM_SEGMENTS = 4

# Define channels with explicit IDs
channels = [
    {"id": 1, "name": "Facebook Ads", "type": "Social", "cost_model": "CPC"},
    {"id": 2, "name": "Google Ads", "type": "Search", "cost_model": "CPC"},
    {"id": 3, "name": "Email Marketing", "type": "Email", "cost_model": "CPM"},
    {"id": 4, "name": "Instagram", "type": "Social", "cost_model": "CPM"},
    {"id": 5, "name": "LinkedIn", "type": "Social", "cost_model": "CPC"}
]

# Define segments with explicit IDs
segments = [
    {"id": 1, "name": "New Visitors", "description": "First-time visitors to the site"},
    {"id": 2, "name": "Returning Visitors", "description": "Visitors who have been to the site before"},
    {"id": 3, "name": "High-Value Customers", "description": "Customers with high lifetime value"},
    {"id": 4, "name": "Cart Abandoners", "description": "Users who added to cart but didn't purchase"}
]

# Event types by channel
channel_event_types = {
    "Facebook Ads": ["impression", "click", "page_view", "add_to_cart", "purchase"],
    "Google Ads": ["impression", "click", "page_view", "add_to_cart", "purchase"],
    "Email Marketing": ["open", "click", "page_view", "add_to_cart", "purchase"],
    "Instagram": ["impression", "click", "page_view", "add_to_cart", "purchase"],
    "LinkedIn": ["impression", "click", "page_view", "add_to_cart", "purchase"]
}

# Device types and browsers
device_types = ["Mobile", "Desktop", "Tablet"]
browsers = ["Chrome", "Safari", "Firefox", "Edge"]
locations = ["United States", "United Kingdom", "Canada", "Germany", "France", "Australia", "Japan"]

# Campaign name templates
campaign_templates = [
    "{season} {year} {product} Campaign",
    "{product} {audience} Promotion {year}",
    "{audience} Retargeting {quarter} {year}",
    "New {product} Launch {month} {year}",
    "{holiday} Special {year}"
]

products = ["Shoes", "Apparel", "Electronics", "Home Goods", "Beauty", "Fitness"]
audiences = ["Youth", "Adult", "Senior", "Family", "Professional"]
seasons = ["Spring", "Summer", "Fall", "Winter"]
holidays = ["Christmas", "Black Friday", "Cyber Monday", "Valentine's Day", "Back to School"]
months = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
quarters = ["Q1", "Q2", "Q3", "Q4"]

def create_campaign_name():
    template = random.choice(campaign_templates)
    year = random.choice([2022, 2023, 2024])
    product = random.choice(products)
    audience = random.choice(audiences)
    season = random.choice(seasons)
    holiday = random.choice(holidays)
    month = random.choice(months)
    quarter = random.choice(quarters)
    
    return template.format(
        year=year,
        product=product,
        audience=audience,
        season=season,
        holiday=holiday,
        month=month,
        quarter=quarter
    )

def generate_campaign_data(channel_ids):
    """Generate synthetic campaign data"""
    campaigns = []
    for i in range(1, NUM_CAMPAIGNS + 1):
        # Random start date within the last 90 days
        days_ago = random.randint(0, 80)
        start_date = END_DATE - timedelta(days=days_ago)
        
        # Random duration between 5 and 30 days
        duration = random.randint(5, 30)
        end_date = start_date + timedelta(days=duration)
        
        # If campaign ends after today, set status to active, otherwise completed
        status = "active" if end_date > END_DATE else "completed"
        
        # Random budget between $1,000 and $50,000
        budget = round(random.uniform(1000, 50000), 2)
        
        # If completed, spend near budget, otherwise partial spend
        if status == "completed":
            spend_factor = random.uniform(0.85, 1.05)  # 85% to 105% of budget
        else:
            elapsed = (END_DATE - start_date).days
            total = duration
            spend_factor = (elapsed / total) * random.uniform(0.9, 1.1)
        
        spend_to_date = round(budget * spend_factor, 2)
        
        campaigns.append({
            "id": i,
            "name": create_campaign_name(),
            "channel_id": random.choice(channel_ids),
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "budget": budget,
            "spend": spend_to_date,
            "status": status
        })
    
    return campaigns

def generate_user_segment_assignments(user_ids, segment_ids):
    """Assign users to segments"""
    user_segments = []
    
    # Every user gets at least one segment
    for user_id in user_ids:
        # Random number of segments per user (1 to 2)
        num_segments = random.randint(1, 2)
        
        # Randomly select segments for this user
        user_segment_ids = random.sample(segment_ids, num_segments)
        
        for segment_id in user_segment_ids:
            user_segments.append({
                "user_id": user_id,
                "segment_id": segment_id
            })
    
    return user_segments

def generate_user_events(user_ids, campaign_mapping, channel_names_by_id):
    """Generate user events"""
    events = []
    total_events = 1000  # Reduced for testing
    
    for i in range(total_events):
        user_id = random.choice(user_ids)
        
        # Select random date within range
        days_ago = random.randint(0, 89)
        event_date = END_DATE - timedelta(days=days_ago)
        
        # Add random time
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        event_timestamp = event_date.replace(hour=hours, minute=minutes, second=seconds)
        
        # Select random campaign and channel
        campaign_id, channel_id = random.choice(list(campaign_mapping.items()))
        channel_name = channel_names_by_id[channel_id]
        
        # Select event type based on channel
        event_types = channel_event_types.get(channel_name, ["impression", "click"])
        event_name = random.choice(event_types)
        
        # Generate additional properties
        device_type = random.choice(device_types)
        browser = random.choice(browsers)
        location = random.choice(locations)
        
        # Different referrers based on channel
        if "Facebook" in channel_name or "Instagram" in channel_name:
            referrer = "social_media"
        elif "Google" in channel_name:
            referrer = "search"
        elif "Email" in channel_name:
            referrer = "email"
        else:
            referrer = random.choice(["direct", "organic", "referral"])
        
        # Event properties as JSON
        properties = {
            "screen_size": random.choice(["small", "medium", "large"]),
            "os": random.choice(["iOS", "Android", "Windows", "MacOS"]),
            "session_id": f"session_{random.randint(10000, 99999)}"
        }
        
        events.append({
            "id": i + 1,
            "user_id": user_id,
            "name": event_name,
            "timestamp": event_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "campaign_id": campaign_id,
            "channel_id": channel_id,
            "referrer": referrer,
            "device": device_type,
            "browser": browser,
            "location": location,
            "properties": json.dumps(properties)
        })
    
    return events

def generate_conversions(user_ids, campaign_mapping):
    """Generate conversion data"""
    conversions = []
    conversion_count = int(len(user_ids) * 0.15)  # 15% conversion rate
    
    # Conversion types and their value ranges
    conversion_types = {
        "purchase": (50, 500),
        "subscription": (10, 100),
        "lead": (5, 50)
    }
    
    for i in range(conversion_count):
        user_id = random.choice(user_ids)
        
        # Select random date within range
        days_ago = random.randint(0, 89)
        conv_date = END_DATE - timedelta(days=days_ago)
        
        # Add random time
        hours = random.randint(0, 23)
        minutes = random.randint(0, 59)
        seconds = random.randint(0, 59)
        conv_timestamp = conv_date.replace(hour=hours, minute=minutes, second=seconds)
        
        # Select random campaign and channel
        campaign_id, channel_id = random.choice(list(campaign_mapping.items()))
        
        # Select conversion type and generate appropriate value
        conv_type, value_range = random.choice(list(conversion_types.items()))
        conversion_value = round(random.uniform(value_range[0], value_range[1]), 2)
        
        conversions.append({
            "id": i + 1,
            "user_id": user_id,
            "type": conv_type,
            "value": conversion_value,
            "campaign_id": campaign_id,
            "channel_id": channel_id,
            "timestamp": conv_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return conversions

def create_schema_if_not_exists():
    """Create required schemas if they don't exist"""
    with engine.begin() as connection:
        # Create schemas
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))

def drop_tables_if_exist():
    """Drop existing tables to start fresh"""
    with engine.begin() as connection:
        # Drop views first to avoid dependency issues
        connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_campaign_performance CASCADE;"))
        connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics.daily_channel_performance CASCADE;"))
        connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics.segment_performance CASCADE;"))
        connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics.segment_cac CASCADE;"))
        connection.execute(text("DROP MATERIALIZED VIEW IF EXISTS analytics.channel_roas CASCADE;"))
        
        # Drop tables
        connection.execute(text("DROP TABLE IF EXISTS analytics.conversions CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS raw.user_events CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS analytics.user_segments CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS analytics.campaigns CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS analytics.segments CASCADE;"))
        connection.execute(text("DROP TABLE IF EXISTS analytics.channels CASCADE;"))

def create_tables():
    """Create tables"""
    with engine.begin() as connection:
        # Create channels table
        connection.execute(text("""
        CREATE TABLE analytics.channels (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            type VARCHAR(50) NOT NULL,
            cost_model VARCHAR(50)
        );
        """))
        
        # Create segments table
        connection.execute(text("""
        CREATE TABLE analytics.segments (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            description TEXT
        );
        """))
        
        # Create campaigns table
        connection.execute(text("""
        CREATE TABLE analytics.campaigns (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            channel_id INTEGER REFERENCES analytics.channels(id),
            start_date DATE NOT NULL,
            end_date DATE,
            budget DECIMAL(12, 2) NOT NULL,
            spend DECIMAL(12, 2) DEFAULT 0.00,
            status VARCHAR(50) DEFAULT 'active'
        );
        """))
        
        # Create user segments table
        connection.execute(text("""
        CREATE TABLE analytics.user_segments (
            user_id VARCHAR(100) NOT NULL,
            segment_id INTEGER REFERENCES analytics.segments(id),
            PRIMARY KEY (user_id, segment_id)
        );
        """))
        
        # Create user events table
        connection.execute(text("""
        CREATE TABLE raw.user_events (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL,
            name VARCHAR(100) NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            campaign_id INTEGER REFERENCES analytics.campaigns(id),
            channel_id INTEGER REFERENCES analytics.channels(id),
            referrer VARCHAR(255),
            device VARCHAR(50),
            browser VARCHAR(50),
            location VARCHAR(100),
            properties JSONB
        );
        """))
        
        # Create conversions table
        connection.execute(text("""
        CREATE TABLE analytics.conversions (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(100) NOT NULL,
            type VARCHAR(100) NOT NULL,
            value DECIMAL(12, 2),
            campaign_id INTEGER REFERENCES analytics.campaigns(id),
            channel_id INTEGER REFERENCES analytics.channels(id),
            timestamp TIMESTAMP NOT NULL
        );
        """))

def insert_channels_data(channels):
    """Insert channels data using SQL"""
    with engine.begin() as connection:
        for channel in channels:
            connection.execute(
                text("INSERT INTO analytics.channels (id, name, type, cost_model) VALUES (:id, :name, :type, :cost_model)"),
                channel
            )
    print(f"Inserted {len(channels)} channels")
    
def insert_segments_data(segments):
    """Insert segments data using SQL"""
    with engine.begin() as connection:
        for segment in segments:
            connection.execute(
                text("INSERT INTO analytics.segments (id, name, description) VALUES (:id, :name, :description)"),
                segment
            )
    print(f"Inserted {len(segments)} segments")

def insert_campaigns_data(campaigns):
    """Insert campaigns data using SQL"""
    with engine.begin() as connection:
        for campaign in campaigns:
            connection.execute(
                text("""
                INSERT INTO analytics.campaigns 
                (id, name, channel_id, start_date, end_date, budget, spend, status) 
                VALUES (:id, :name, :channel_id, :start_date, :end_date, :budget, :spend, :status)
                """),
                campaign
            )
    print(f"Inserted {len(campaigns)} campaigns")

def insert_user_segments_data(user_segments):
    """Insert user segments data using SQL"""
    batch_size = 100
    with engine.begin() as connection:
        for i in range(0, len(user_segments), batch_size):
            batch = user_segments[i:i+batch_size]
            for item in batch:
                connection.execute(
                    text("INSERT INTO analytics.user_segments (user_id, segment_id) VALUES (:user_id, :segment_id)"),
                    item
                )
    print(f"Inserted {len(user_segments)} user segment assignments")

def insert_user_events_data(events):
    """Insert events data using SQL"""
    batch_size = 100
    with engine.begin() as connection:
        for i in range(0, len(events), batch_size):
            batch = events[i:i+batch_size]
            for event in batch:
                connection.execute(
                    text("""
                    INSERT INTO raw.user_events 
                    (id, user_id, name, timestamp, campaign_id, channel_id, referrer, device, browser, location, properties) 
                    VALUES (:id, :user_id, :name, :timestamp, :campaign_id, :channel_id, :referrer, :device, :browser, :location, :properties)
                    """),
                    {
                        "id": event["id"],
                        "user_id": event["user_id"],
                        "name": event["name"],
                        "timestamp": event["timestamp"],
                        "campaign_id": event["campaign_id"],
                        "channel_id": event["channel_id"],
                        "referrer": event["referrer"],
                        "device": event["device"],
                        "browser": event["browser"],
                        "location": event["location"],
                        "properties": event["properties"]
                    }
                )
            print(f"Inserted events batch {i//batch_size + 1}/{(len(events)-1)//batch_size + 1}")
    print(f"Inserted {len(events)} events")

def insert_conversions_data(conversions):
    """Insert conversions data using SQL"""
    batch_size = 100
    with engine.begin() as connection:
        for i in range(0, len(conversions), batch_size):
            batch = conversions[i:i+batch_size]
            for conversion in batch:
                connection.execute(
                    text("""
                    INSERT INTO analytics.conversions 
                    (id, user_id, type, value, campaign_id, channel_id, timestamp) 
                    VALUES (:id, :user_id, :type, :value, :campaign_id, :channel_id, :timestamp)
                    """),
                    conversion
                )
    print(f"Inserted {len(conversions)} conversions")

def create_materialized_views():
    """Create the materialized views for metrics"""
    with engine.begin() as connection:
        # Daily Campaign Performance
        connection.execute(text("""
        CREATE MATERIALIZED VIEW analytics.daily_campaign_performance AS
        SELECT
            DATE(c.timestamp) AS date,
            c.campaign_id,
            ca.name AS campaign_name,
            ca.channel_id,
            ch.name AS channel_name,
            COUNT(DISTINCT c.user_id) AS conversions,
            SUM(c.value) AS total_conversion_value,
            CASE 
                WHEN COUNT(DISTINCT c.user_id) > 0 
                THEN SUM(c.value) / COUNT(DISTINCT c.user_id) 
                ELSE 0 
            END AS avg_conversion_value
        FROM 
            analytics.conversions c
        JOIN 
            analytics.campaigns ca ON c.campaign_id = ca.id
        JOIN 
            analytics.channels ch ON ca.channel_id = ch.id
        GROUP BY 
            DATE(c.timestamp), 
            c.campaign_id, 
            ca.name, 
            ca.channel_id, 
            ch.name;
        """))
        
        # Daily Channel Performance
        connection.execute(text("""
        CREATE MATERIALIZED VIEW analytics.daily_channel_performance AS
        SELECT
            DATE(e.timestamp) AS date,
            e.channel_id,
            ch.name AS channel_name,
            COUNT(*) AS events,
            COUNT(DISTINCT e.user_id) AS unique_users,
            SUM(CASE WHEN e.name = 'click' THEN 1 ELSE 0 END) AS clicks,
            SUM(CASE WHEN e.name = 'impression' THEN 1 ELSE 0 END) AS impressions,
            CASE 
                WHEN SUM(CASE WHEN e.name = 'impression' THEN 1 ELSE 0 END) > 0 
                THEN SUM(CASE WHEN e.name = 'click' THEN 1 ELSE 0 END)::FLOAT / 
                    SUM(CASE WHEN e.name = 'impression' THEN 1 ELSE 0 END) 
                ELSE 0 
            END AS ctr
        FROM 
            raw.user_events e
        JOIN 
            analytics.channels ch ON e.channel_id = ch.id
        GROUP BY 
            DATE(e.timestamp), 
            e.channel_id, 
            ch.name;
        """))
        
        # Segment Performance
        connection.execute(text("""
        CREATE MATERIALIZED VIEW analytics.segment_performance AS
        SELECT
            DATE(c.timestamp) AS date,
            us.segment_id,
            s.name AS segment_name,
            COUNT(DISTINCT c.user_id) AS conversions,
            SUM(c.value) AS total_conversion_value,
            CASE 
                WHEN COUNT(DISTINCT c.user_id) > 0 
                THEN SUM(c.value) / COUNT(DISTINCT c.user_id) 
                ELSE 0 
            END AS avg_conversion_value
        FROM 
            analytics.conversions c
        JOIN 
            analytics.user_segments us ON c.user_id = us.user_id
        JOIN 
            analytics.segments s ON us.segment_id = s.id
        GROUP BY 
            DATE(c.timestamp), 
            us.segment_id, 
            s.name;
        """))
        
        # Segment CAC (Simplified calculation without date extraction)
        connection.execute(text("""
        CREATE MATERIALIZED VIEW analytics.segment_cac AS
        WITH segment_costs AS (
            SELECT
                DATE(e.timestamp) AS date,
                us.segment_id,
                s.name AS segment_name,
                ca.id AS campaign_id,
                SUM(ca.spend) / 30.0 AS daily_spend  -- Simple average over 30 days
            FROM
                raw.user_events e
            JOIN
                analytics.campaigns ca ON e.campaign_id = ca.id
            JOIN
                analytics.user_segments us ON e.user_id = us.user_id
            JOIN
                analytics.segments s ON us.segment_id = s.id
            GROUP BY
                DATE(e.timestamp), us.segment_id, s.name, ca.id, ca.spend
        ),
        segment_acquisitions AS (
            SELECT
                DATE(c.timestamp) AS date,
                us.segment_id,
                COUNT(DISTINCT c.user_id) AS acquisitions
            FROM
                analytics.conversions c
            JOIN
                analytics.user_segments us ON c.user_id = us.user_id
            WHERE
                c.type = 'purchase'
            GROUP BY
                DATE(c.timestamp), us.segment_id
        )
        SELECT
            sc.date,
            sc.segment_id,
            sc.segment_name,
            CASE 
                WHEN COALESCE(sa.acquisitions, 0) > 0 
                THEN SUM(sc.daily_spend) / sa.acquisitions 
                ELSE 0 
            END AS cac
        FROM
            segment_costs sc
        LEFT JOIN
            segment_acquisitions sa ON sc.date = sa.date AND sc.segment_id = sa.segment_id
        GROUP BY
            sc.date, sc.segment_id, sc.segment_name, sa.acquisitions;
        """))
        
        # Channel ROAS (Simplified calculation without date extraction)
        connection.execute(text("""
        CREATE MATERIALIZED VIEW analytics.channel_roas AS
        WITH channel_spend AS (
            SELECT
                DATE(e.timestamp) AS date,
                ch.id AS channel_id,
                ch.name AS channel_name,
                SUM(ca.spend) / 30.0 AS daily_spend  -- Simple average over 30 days
            FROM
                raw.user_events e
            JOIN
                analytics.campaigns ca ON e.campaign_id = ca.id
            JOIN
                analytics.channels ch ON e.channel_id = ch.id
            GROUP BY
                DATE(e.timestamp), ch.id, ch.name, ca.spend
        ),
        channel_revenue AS (
            SELECT
                DATE(c.timestamp) AS date,
                ch.id AS channel_id,
                SUM(c.value) AS revenue
            FROM
                analytics.conversions c
            JOIN
                analytics.channels ch ON c.channel_id = ch.id
            GROUP BY
                DATE(c.timestamp), ch.id
        )
        SELECT
            cs.date,
            cs.channel_id,
            cs.channel_name,
            CASE 
                WHEN SUM(cs.daily_spend) > 0 
                THEN COALESCE(cr.revenue, 0) / SUM(cs.daily_spend) 
                ELSE 0 
            END AS roas
        FROM
            channel_spend cs
        LEFT JOIN
            channel_revenue cr ON cs.date = cr.date AND cs.channel_id = cr.channel_id
        GROUP BY
            cs.date, cs.channel_id, cs.channel_name, cr.revenue;
        """))

def main():
    """Main function to generate and save all data"""
    # Create schemas if they don't exist and drop existing tables
    create_schema_if_not_exists()
    drop_tables_if_exist()
    create_tables()
    
    # Insert channels and segments
    insert_channels_data(channels)
    insert_segments_data(segments)
    
    # Get channel IDs
    channel_ids = [c["id"] for c in channels]
    segment_ids = [s["id"] for s in segments]
    
    # Create a mapping of channel ID to name
    channel_names_by_id = {c["id"]: c["name"] for c in channels}
    
    # Generate campaigns
    campaigns = generate_campaign_data(channel_ids)
    insert_campaigns_data(campaigns)
    
    # Create campaign to channel mapping
    campaign_mapping = {c["id"]: c["channel_id"] for c in campaigns}
    
    # Generate user IDs
    user_ids = [f"user_{i}" for i in range(1, NUM_USERS+1)]
    
    # Assign users to segments
    user_segments = generate_user_segment_assignments(user_ids, segment_ids)
    insert_user_segments_data(user_segments)
    
    # Generate events and conversions
    events = generate_user_events(user_ids, campaign_mapping, channel_names_by_id)
    conversions = generate_conversions(user_ids, campaign_mapping)
    
    # Insert events and conversions
    insert_user_events_data(events)
    insert_conversions_data(conversions)
    
    # Create materialized views
    create_materialized_views()
    
    print("Data generation completed successfully!")

if __name__ == "__main__":
    main() 