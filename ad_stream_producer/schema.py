from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class AdEvent(BaseModel):
    event_id: str
    event_time: datetime
    user_id: str
    campaign_id: int
    ad_id: int
    device: str
    country: str
    event_type: str
    ad_creative_id: Optional[str] = None