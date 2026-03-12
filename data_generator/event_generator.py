import uuid
import random
from datetime import datetime, timezone
from faker import Faker


class EventGenerator:
    def __init__(self):
        self.faker = Faker()

    def generate_event(self):
        event = {
            "event_id": str(uuid.uuid4()),
            "event_time": datetime.now(timezone.utc).isoformat(),
            "user_id": str(uuid.uuid4()),
            "campaign_id": random.randint(1, 100),
            "ad_id": random.randint(1, 500),
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "country": self.faker.country_code(),
            "event_type": random.choice(["click", "view", "purchase"]),
            "ad_creative_id": str(uuid.uuid4()) if random.random() > 0.5 else None 
        }
        return event