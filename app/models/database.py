import os
from sqlalchemy import create_engine, Column, Integer, String, Float, Date, DateTime, ForeignKey, Table, MetaData, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime

# Get database URL from environment variable or use default
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://airflow:airflow@postgres/airflow')

# Create SQLAlchemy engine and session
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# Data models
class Channel(Base):
    __tablename__ = 'channels'
    __table_args__ = {'schema': 'analytics'}
    
    channel_id = Column(Integer, primary_key=True)
    channel_name = Column(String, nullable=False)
    channel_type = Column(String, nullable=False)
    cost_model = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    campaigns = relationship("Campaign", back_populates="channel")


class Campaign(Base):
    __tablename__ = 'campaigns'
    __table_args__ = {'schema': 'analytics'}
    
    campaign_id = Column(Integer, primary_key=True)
    campaign_name = Column(String, nullable=False)
    channel_id = Column(Integer, ForeignKey('analytics.channels.channel_id'))
    start_date = Column(Date, nullable=False)
    end_date = Column(Date)
    budget = Column(Float, nullable=False)
    spend_to_date = Column(Float, default=0.0)
    status = Column(String, default='active')
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    channel = relationship("Channel", back_populates="campaigns")
    events = relationship("UserEvent", back_populates="campaign")
    conversions = relationship("Conversion", back_populates="campaign")


class Segment(Base):
    __tablename__ = 'segments'
    __table_args__ = {'schema': 'analytics'}
    
    segment_id = Column(Integer, primary_key=True)
    segment_name = Column(String, nullable=False)
    segment_description = Column(String)
    segment_rules = Column(String)  # Stored as JSON
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class UserEvent(Base):
    __tablename__ = 'user_events'
    __table_args__ = {'schema': 'raw'}
    
    event_id = Column(Integer, primary_key=True)
    user_id = Column(String, nullable=False)
    event_name = Column(String, nullable=False)
    event_timestamp = Column(DateTime, nullable=False)
    campaign_id = Column(Integer, ForeignKey('analytics.campaigns.campaign_id'))
    channel_id = Column(Integer, ForeignKey('analytics.channels.channel_id'))
    referrer = Column(String)
    device_type = Column(String)
    browser = Column(String)
    location = Column(String)
    event_properties = Column(String)  # Stored as JSON
    created_at = Column(DateTime, default=datetime.now)
    
    campaign = relationship("Campaign", back_populates="events")
    channel = relationship("Channel")


class Conversion(Base):
    __tablename__ = 'conversions'
    __table_args__ = {'schema': 'analytics'}
    
    conversion_id = Column(Integer, primary_key=True)
    user_id = Column(String, nullable=False)
    conversion_type = Column(String, nullable=False)
    conversion_value = Column(Float)
    campaign_id = Column(Integer, ForeignKey('analytics.campaigns.campaign_id'))
    channel_id = Column(Integer, ForeignKey('analytics.channels.channel_id'))
    conversion_timestamp = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    
    campaign = relationship("Campaign", back_populates="conversions")
    channel = relationship("Channel")


# Helper functions for database interactions
def get_db():
    """Get a database session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_metrics(start_date, end_date, channel_id=None, segment_id=None):
    """Get metrics for a specific date range, channel, and/or segment"""
    
    with engine.connect() as connection:
        # Base query for campaign performance
        campaign_query = text("""
            SELECT
                date,
                campaign_id,
                campaign_name,
                channel_id,
                channel_name,
                conversions,
                total_conversion_value,
                avg_conversion_value
            FROM analytics.daily_campaign_performance
            WHERE date BETWEEN :start_date AND :end_date
            {}
            ORDER BY date, campaign_id
        """.format("AND channel_id = :channel_id" if channel_id else ""))
        
        # Base query for channel performance
        channel_query = text("""
            SELECT
                date,
                channel_id,
                channel_name,
                events,
                unique_users,
                clicks,
                impressions,
                ctr
            FROM analytics.daily_channel_performance
            WHERE date BETWEEN :start_date AND :end_date
            {}
            ORDER BY date, channel_id
        """.format("AND channel_id = :channel_id" if channel_id else ""))
        
        # Base query for segment performance
        segment_query = text("""
            SELECT
                date,
                segment_id,
                segment_name,
                conversions,
                total_conversion_value,
                avg_conversion_value
            FROM analytics.segment_performance
            WHERE date BETWEEN :start_date AND :end_date
            {}
            ORDER BY date, segment_id
        """.format("AND segment_id = :segment_id" if segment_id else ""))
        
        # Base query for CAC by segment
        cac_query = text("""
            SELECT
                date,
                segment_id,
                segment_name,
                cac
            FROM analytics.segment_cac
            WHERE date BETWEEN :start_date AND :end_date
            {}
            ORDER BY date, segment_id
        """.format("AND segment_id = :segment_id" if segment_id else ""))
        
        # Base query for ROAS by channel
        roas_query = text("""
            SELECT
                date,
                channel_id,
                channel_name,
                roas
            FROM analytics.channel_roas
            WHERE date BETWEEN :start_date AND :end_date
            {}
            ORDER BY date, channel_id
        """.format("AND channel_id = :channel_id" if channel_id else ""))
        
        # Execute all queries with parameters
        params = {
            'start_date': start_date,
            'end_date': end_date
        }
        
        if channel_id:
            params['channel_id'] = channel_id
        
        if segment_id:
            params['segment_id'] = segment_id
        
        campaign_result = connection.execute(campaign_query, params).fetchall()
        channel_result = connection.execute(channel_query, params).fetchall()
        segment_result = connection.execute(segment_query, params).fetchall()
        cac_result = connection.execute(cac_query, params).fetchall()
        roas_result = connection.execute(roas_query, params).fetchall()
        
        # Process results into a dictionary
        metrics = {
            'campaign_performance': [dict(row) for row in campaign_result],
            'channel_performance': [dict(row) for row in channel_result],
            'segment_performance': [dict(row) for row in segment_result],
            'segment_cac': [dict(row) for row in cac_result],
            'channel_roas': [dict(row) for row in roas_result]
        }
        
        return metrics 