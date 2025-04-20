-- Create dedicated database for the marketing dashboard
CREATE DATABASE marketing_dashboard;

-- Connect to the new database
\c marketing_dashboard

-- Create schemas
CREATE SCHEMA raw;
CREATE SCHEMA archive;
CREATE SCHEMA analytics;

-- Create necessary tables for the marketing dashboard

-- Campaigns Table
CREATE TABLE analytics.campaigns (
    campaign_id SERIAL PRIMARY KEY,
    campaign_name VARCHAR(255) NOT NULL,
    channel_id INTEGER NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    budget DECIMAL(12, 2) NOT NULL,
    spend_to_date DECIMAL(12, 2) DEFAULT 0.00,
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Channels Table
CREATE TABLE analytics.channels (
    channel_id SERIAL PRIMARY KEY,
    channel_name VARCHAR(100) NOT NULL,
    channel_type VARCHAR(50) NOT NULL,
    cost_model VARCHAR(50), -- CPC, CPM, etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User Segments Table
CREATE TABLE analytics.segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(100) NOT NULL,
    segment_description TEXT,
    segment_rules JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- User Events Table
CREATE TABLE raw.user_events (
    event_id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    event_name VARCHAR(100) NOT NULL,
    event_timestamp TIMESTAMP NOT NULL,
    campaign_id INTEGER,
    channel_id INTEGER,
    referrer VARCHAR(255),
    device_type VARCHAR(50),
    browser VARCHAR(50),
    location VARCHAR(100),
    event_properties JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (event_timestamp);

-- Create daily partitions for user events (example for a week)
CREATE TABLE raw.user_events_p2023_01_01 PARTITION OF raw.user_events
    FOR VALUES FROM ('2023-01-01') TO ('2023-01-02');

-- User Segments Junction Table
CREATE TABLE analytics.user_segments (
    user_id VARCHAR(100) NOT NULL,
    segment_id INTEGER NOT NULL,
    assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (user_id, segment_id)
);

-- Conversions Table
CREATE TABLE analytics.conversions (
    conversion_id SERIAL PRIMARY KEY,
    user_id VARCHAR(100) NOT NULL,
    conversion_type VARCHAR(100) NOT NULL,
    conversion_value DECIMAL(12, 2),
    campaign_id INTEGER,
    channel_id INTEGER,
    conversion_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) PARTITION BY RANGE (conversion_timestamp);

-- Create daily partitions for conversions (example for a day)
CREATE TABLE analytics.conversions_p2023_01_01 PARTITION OF analytics.conversions
    FOR VALUES FROM ('2023-01-01') TO ('2023-01-02');

-- Materialized View for Daily Campaign Performance
CREATE MATERIALIZED VIEW analytics.daily_campaign_performance AS
SELECT
    DATE(c.conversion_timestamp) AS date,
    c.campaign_id,
    ca.campaign_name,
    ca.channel_id,
    ch.channel_name,
    COUNT(DISTINCT c.user_id) AS conversions,
    SUM(c.conversion_value) AS total_conversion_value,
    SUM(c.conversion_value) / COUNT(DISTINCT c.user_id) AS avg_conversion_value
FROM 
    analytics.conversions c
JOIN 
    analytics.campaigns ca ON c.campaign_id = ca.campaign_id
JOIN 
    analytics.channels ch ON ca.channel_id = ch.channel_id
GROUP BY 
    DATE(c.conversion_timestamp), 
    c.campaign_id, 
    ca.campaign_name, 
    ca.channel_id, 
    ch.channel_name;

-- Materialized View for Daily Channel Performance
CREATE MATERIALIZED VIEW analytics.daily_channel_performance AS
SELECT
    DATE(e.event_timestamp) AS date,
    e.channel_id,
    ch.channel_name,
    COUNT(*) AS events,
    COUNT(DISTINCT e.user_id) AS unique_users,
    SUM(CASE WHEN e.event_name = 'click' THEN 1 ELSE 0 END) AS clicks,
    SUM(CASE WHEN e.event_name = 'impression' THEN 1 ELSE 0 END) AS impressions,
    CASE 
        WHEN SUM(CASE WHEN e.event_name = 'impression' THEN 1 ELSE 0 END) > 0 
        THEN SUM(CASE WHEN e.event_name = 'click' THEN 1 ELSE 0 END)::FLOAT / 
             SUM(CASE WHEN e.event_name = 'impression' THEN 1 ELSE 0 END) 
        ELSE 0 
    END AS ctr
FROM 
    raw.user_events e
JOIN 
    analytics.channels ch ON e.channel_id = ch.channel_id
GROUP BY 
    DATE(e.event_timestamp), 
    e.channel_id, 
    ch.channel_name;

-- Materialized View for Segment Performance
CREATE MATERIALIZED VIEW analytics.segment_performance AS
SELECT
    DATE(c.conversion_timestamp) AS date,
    us.segment_id,
    s.segment_name,
    COUNT(DISTINCT c.user_id) AS conversions,
    SUM(c.conversion_value) AS total_conversion_value,
    AVG(c.conversion_value) AS avg_conversion_value
FROM 
    analytics.conversions c
JOIN 
    analytics.user_segments us ON c.user_id = us.user_id
JOIN 
    analytics.segments s ON us.segment_id = s.segment_id
GROUP BY 
    DATE(c.conversion_timestamp), 
    us.segment_id, 
    s.segment_name;

-- Add Foreign Keys
ALTER TABLE analytics.campaigns 
    ADD CONSTRAINT fk_channel_id FOREIGN KEY (channel_id) REFERENCES analytics.channels (channel_id);

ALTER TABLE raw.user_events 
    ADD CONSTRAINT fk_campaign_id FOREIGN KEY (campaign_id) REFERENCES analytics.campaigns (campaign_id),
    ADD CONSTRAINT fk_channel_id FOREIGN KEY (channel_id) REFERENCES analytics.channels (channel_id);

ALTER TABLE analytics.user_segments 
    ADD CONSTRAINT fk_segment_id FOREIGN KEY (segment_id) REFERENCES analytics.segments (segment_id);

ALTER TABLE analytics.conversions 
    ADD CONSTRAINT fk_campaign_id FOREIGN KEY (campaign_id) REFERENCES analytics.campaigns (campaign_id),
    ADD CONSTRAINT fk_channel_id FOREIGN KEY (channel_id) REFERENCES analytics.channels (channel_id); 