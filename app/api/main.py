from fastapi import FastAPI, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from datetime import date, datetime, timedelta
from typing import List, Optional
from pydantic import BaseModel
import json

# Import our database models
from app.models.database import get_db, get_metrics, Channel, Campaign, Segment

app = FastAPI(title="Marketing Analytics API", 
              description="API for marketing analytics dashboard")

# Pydantic models for responses
class ChannelResponse(BaseModel):
    channel_id: int
    channel_name: str
    channel_type: str
    cost_model: Optional[str] = None
    
    class Config:
        orm_mode = True

class CampaignResponse(BaseModel):
    campaign_id: int
    campaign_name: str
    channel_id: int
    start_date: date
    end_date: Optional[date] = None
    budget: float
    spend_to_date: float
    status: str
    
    class Config:
        orm_mode = True

class SegmentResponse(BaseModel):
    segment_id: int
    segment_name: str
    segment_description: Optional[str] = None
    
    class Config:
        orm_mode = True

class MetricsResponse(BaseModel):
    campaign_performance: List[dict]
    channel_performance: List[dict]
    segment_performance: List[dict]
    segment_cac: List[dict]
    channel_roas: List[dict]

# API endpoints
@app.get("/", tags=["Root"])
def read_root():
    return {"message": "Welcome to the Marketing Analytics API"}

@app.get("/channels/", response_model=List[ChannelResponse], tags=["Channels"])
def get_channels(db: Session = Depends(get_db)):
    channels = db.query(Channel).all()
    return channels

@app.get("/channels/{channel_id}", response_model=ChannelResponse, tags=["Channels"])
def get_channel(channel_id: int, db: Session = Depends(get_db)):
    channel = db.query(Channel).filter(Channel.channel_id == channel_id).first()
    if not channel:
        raise HTTPException(status_code=404, detail="Channel not found")
    return channel

@app.get("/campaigns/", response_model=List[CampaignResponse], tags=["Campaigns"])
def get_campaigns(channel_id: Optional[int] = None, db: Session = Depends(get_db)):
    query = db.query(Campaign)
    if channel_id:
        query = query.filter(Campaign.channel_id == channel_id)
    campaigns = query.all()
    return campaigns

@app.get("/campaigns/{campaign_id}", response_model=CampaignResponse, tags=["Campaigns"])
def get_campaign(campaign_id: int, db: Session = Depends(get_db)):
    campaign = db.query(Campaign).filter(Campaign.campaign_id == campaign_id).first()
    if not campaign:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return campaign

@app.get("/segments/", response_model=List[SegmentResponse], tags=["Segments"])
def get_segments(db: Session = Depends(get_db)):
    segments = db.query(Segment).all()
    return segments

@app.get("/segments/{segment_id}", response_model=SegmentResponse, tags=["Segments"])
def get_segment(segment_id: int, db: Session = Depends(get_db)):
    segment = db.query(Segment).filter(Segment.segment_id == segment_id).first()
    if not segment:
        raise HTTPException(status_code=404, detail="Segment not found")
    return segment

@app.get("/metrics/", response_model=MetricsResponse, tags=["Metrics"])
def get_all_metrics(
    start_date: date = Query(None, description="Start date for metrics"),
    end_date: date = Query(None, description="End date for metrics"),
    channel_id: Optional[int] = Query(None, description="Filter by channel ID"),
    segment_id: Optional[int] = Query(None, description="Filter by segment ID"),
):
    # Default to last 30 days if no dates provided
    if not start_date:
        start_date = datetime.now().date() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now().date()
        
    metrics = get_metrics(start_date, end_date, channel_id, segment_id)
    return metrics

@app.get("/metrics/campaigns/", tags=["Metrics"])
def get_campaign_metrics(
    start_date: date = Query(None, description="Start date for metrics"),
    end_date: date = Query(None, description="End date for metrics"),
    channel_id: Optional[int] = Query(None, description="Filter by channel ID"),
):
    # Default to last 30 days if no dates provided
    if not start_date:
        start_date = datetime.now().date() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now().date()
        
    metrics = get_metrics(start_date, end_date, channel_id)
    return {"campaign_performance": metrics["campaign_performance"]}

@app.get("/metrics/channels/", tags=["Metrics"])
def get_channel_metrics(
    start_date: date = Query(None, description="Start date for metrics"),
    end_date: date = Query(None, description="End date for metrics"),
    channel_id: Optional[int] = Query(None, description="Filter by channel ID"),
):
    # Default to last 30 days if no dates provided
    if not start_date:
        start_date = datetime.now().date() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now().date()
        
    metrics = get_metrics(start_date, end_date, channel_id)
    return {
        "channel_performance": metrics["channel_performance"],
        "channel_roas": metrics["channel_roas"]
    }

@app.get("/metrics/segments/", tags=["Metrics"])
def get_segment_metrics(
    start_date: date = Query(None, description="Start date for metrics"),
    end_date: date = Query(None, description="End date for metrics"),
    segment_id: Optional[int] = Query(None, description="Filter by segment ID"),
):
    # Default to last 30 days if no dates provided
    if not start_date:
        start_date = datetime.now().date() - timedelta(days=30)
    if not end_date:
        end_date = datetime.now().date()
        
    metrics = get_metrics(start_date, end_date, segment_id=segment_id)
    return {
        "segment_performance": metrics["segment_performance"],
        "segment_cac": metrics["segment_cac"]
    } 