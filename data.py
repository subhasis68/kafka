from faker import Faker
from faker.providers import BaseProvider
import time
import random
from datetime import datetime, timezone

class TimestampProvider(BaseProvider):
    def timestamp(self, unix=True):
        if unix:
            return int(time.time()) - random.randint(0, 10**6)
        else:
            return fake.date_time().isoformat()

fake = Faker()
fake.add_provider(TimestampProvider)

def get_live_data():
    return {
        "vehicle_id" : random.randint(1, 5),
        "lat" : round(float(fake.latitude()), 2),
        "long" : round(float(fake.longitude()), 2),
        "speed" : round(random.uniform(20, 120), 2),
        "temperature" : round(random.uniform(-30, 45), 1),
        "humidity" : random.randint(0, 100),
        #"created_at" : datetime.now().strftime("%Y-%m-%d~%H:%M:%S"),
        #"created_at" : datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
        "created_at": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
    }

if __name__ == "__main__":
    print(get_live_data())