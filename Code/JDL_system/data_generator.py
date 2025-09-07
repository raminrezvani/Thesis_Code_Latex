import datetime
import random
from typing import Iterator, Tuple

Triple = Tuple[str, str, str]


def generate_synthetic_triples(num_triples: int) -> Iterator[Triple]:
	"""Generate traffic ontology-like triples similar to centralized_agent.

	Each record yields 12 triples. We generate floor(num_triples/12) records.
	Triples approximate those created in centralized_agent.queue_to_rdf.
	"""
	if num_triples <= 0:
		return iter(())

	TARGET_RECORDS = max(1, num_triples // 12)
	LOCATIONS = [
		{"id": "INT1", "lat": 35.6895, "lon": 51.3890},
		{"id": "INT2", "lat": 35.7000, "lon": 51.4000},
		{"id": "HWY1", "lat": 35.7100, "lon": 51.4100},
		{"id": "HWY2", "lat": 35.7200, "lon": 51.4200},
	]
	SENSOR_IDS = [f"SENSOR_{i}" for i in range(1, 21)]
	EVENT_TYPES = ["Normal", "Congestion", "Incident"]
	NUM_VEHICLES = 3000
	SIMULATION_MINUTES = 15

	def random_timestamp_str() -> str:
		now = datetime.datetime.now()
		start_time = now - datetime.timedelta(minutes=SIMULATION_MINUTES)
		time_diff = random.randint(0, SIMULATION_MINUTES * 60)
		return (start_time + datetime.timedelta(seconds=time_diff)).strftime("%Y-%m-%dT%H:%M:%S")

	def get_speed_and_event(hour: int) -> Tuple[float, str]:
		if 7 <= hour <= 9 or 16 <= hour <= 18:
			speed = random.uniform(10, 50)
			event_probs = [0.3, 0.6, 0.1]
		else:
			speed = random.uniform(30, 100)
			event_probs = [0.7, 0.2, 0.1]
		return speed, random.choices(EVENT_TYPES, event_probs)[0]

	traffic = "http://example.org/traffic#"
	city = "http://example.org/cityOnto#"

	def _iter() -> Iterator[Triple]:
		for _ in range(TARGET_RECORDS):
			vehicle_id = f"VEH_{random.randint(1, NUM_VEHICLES)}"
			ts = random_timestamp_str()
			hour = int(ts[11:13])
			loc = random.choice(LOCATIONS)
			speed, event_type = get_speed_and_event(hour)
			sensor_id = random.choice(SENSOR_IDS)

			vehicle_uri = f"{traffic}vehicle/{vehicle_id}"
			sensor_uri = f"{traffic}sensor/{sensor_id}"
			location_uri = f"{traffic}location/{loc['id']}"
			event_uri = f"{traffic}event/{vehicle_id}_{ts.replace(':', '-') }"

			# 1 vehicle type
			yield (vehicle_uri, "rdf:type", f"{traffic}Vehicle")
			# 2 speed
			yield (vehicle_uri, f"{traffic}hasSpeed", f"{speed:.2f}")
			# 3 atLocation
			yield (vehicle_uri, f"{traffic}atLocation", location_uri)
			# 4 detectedBy
			yield (vehicle_uri, f"{traffic}detectedBy", sensor_uri)
			# 5 timestamp
			yield (vehicle_uri, f"{traffic}hasTimestamp", ts)
			# 6 vehicle event type
			yield (vehicle_uri, f"{traffic}hasEventType", event_type)
			# 7 sensor type
			yield (sensor_uri, "rdf:type", f"{traffic}Sensor")
			# 8 location type
			yield (location_uri, "rdf:type", f"{traffic}Location")
			# 9 location lat
			yield (location_uri, f"{city}hasLatitude", f"{loc['lat']}")
			# 10 location lon
			yield (location_uri, f"{city}hasLongitude", f"{loc['lon']}")
			# 11 event type
			yield (event_uri, "rdf:type", f"{traffic}Event")
			# 12 event occursAt
			yield (event_uri, f"{traffic}occursAt", vehicle_uri)

	return _iter()


