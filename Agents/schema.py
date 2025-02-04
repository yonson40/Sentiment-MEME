from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field

class OHLCVData(BaseModel):
    token: str
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: float

class Tweet(BaseModel):
    tweet_id: str
    author_id: Optional[str]
    created_at: datetime
    text: str
    language: Optional[str]
    retweet_count: int = 0
    reply_count: int = 0
    like_count: int = 0
    quote_count: int = 0
    referenced_tweet_id: Optional[str]

class SentimentScore(BaseModel):
    compound_score: float
    positive_score: float
    neutral_score: float
    negative_score: float
    processed_text: str

class TokenSentiment(BaseModel):
    timestamp: datetime
    token: str
    interval: str
    sentiment_mean: float
    sentiment_std: float
    tweet_count: int
    positive_ratio: float
    negative_ratio: float
    neutral_ratio: float
    engagement_score: float

class AgentState(BaseModel):
    """State that is passed between agents"""
    ohlcv_updates: List[OHLCVData] = Field(default_factory=list)
    new_tweets: List[Tweet] = Field(default_factory=list)
    sentiment_scores: Dict[str, SentimentScore] = Field(default_factory=dict)
    token_sentiments: List[TokenSentiment] = Field(default_factory=list)
    error_messages: List[str] = Field(default_factory=list)
    status: str = "initialized"
    last_run: Optional[datetime] = None
