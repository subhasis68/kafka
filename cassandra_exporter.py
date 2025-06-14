from flask import Flask, Response
from cassandra.cluster import Cluster
from datetime import datetime, timedelta, timezone

app = Flask(__name__)

cluster = Cluster(['localhost'], port=9042)  # Replace with your Cassandra nodes if needed
session = cluster.connect('kafkadb')

@app.route("/metrics")
def metrics():
    #since_time = datetime.utcnow() - timedelta(seconds=60)
    # Get 60 seconds ago, as timezone-aware UTC timestamp
    since_time = datetime.now(timezone.utc) - timedelta(seconds=60)

    query = """
        SELECT vehicle_id, speed, temperature, humidity, lat, long, created_at
        FROM vehicle_data
        WHERE created_at > %s ALLOW FILTERING;
    """
    rows = session.execute(query, [since_time])

    # Keep only latest per vehicle
    latest = {}
    for row in rows:
        vid = row.vehicle_id
        if vid not in latest or row.created_at > latest[vid].created_at:
            latest[vid] = row

    output = []
    for vid, row in latest.items():
        labels = f'vehicle_id="{vid}"'
        output.append(f'vehicle_speed{{{labels}}} {row.speed}')
        output.append(f'vehicle_temperature{{{labels}}} {row.temperature}')
        output.append(f'vehicle_humidity{{{labels}}} {row.humidity}')
        output.append(f'vehicle_lat{{{labels}}} {row.lat}')
        output.append(f'vehicle_long{{{labels}}} {row.long}')

    return Response('\n'.join(output), mimetype='text/plain')

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9100)
