from faker import Faker
from faker.providers import BaseProvider
import time
import random

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
        "vehicle_id" : random.randint(0, 100),
        "lat" : round(float(fake.latitude()), 2),
        "long" : round(float(fake.longitude()), 2),
        "speed" : round(random.uniform(20, 120), 2),
        "temperature" : round(random.uniform(-30, 45), 1),
        "humidity" : random.randint(0, 100),
        "created_at" : fake.timestamp(unix=False)
    }

if __name__ == "__main__":
    print(get_live_data())