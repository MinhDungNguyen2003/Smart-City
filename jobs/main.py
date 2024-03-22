import os
import time
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random

LONDON_CONORDINATES = {
    "lat": 51.5074,
    "long": 0.1278  
}

BIRMINGHAM_CONORDINATES = {
    "lat": 52.4862,
    "long": -1.8904
}   

# calculate the increment for the lat and long
LAT_INCREMENT = (BIRMINGHAM_CONORDINATES["lat"] - LONDON_CONORDINATES["lat"]) / 100
LONG_INCREMENT = (BIRMINGHAM_CONORDINATES["long"] - LONDON_CONORDINATES["long"]) / 100

# eviroment variables for configuring 
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFIC_TOPIC = os.getenv("TRAFIC_TOPIC", "trafic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")

start_time = datetime.now()
start_location = LONDON_CONORDINATES.copy()

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def simulate_vehicle_movement():
    global start_location

    # increment the location
    start_location["lat"] += LAT_INCREMENT
    start_location["long"] += LONG_INCREMENT

    # generate the vehicle data
    start_location["lat"] += random.uniform(-0.0005, 0.0005)
    start_location["long"] += random.uniform(-0.0005, 0.0005)

    return start_location 

def generate_vehicle_data(vehicle_id):
    location = simulate_vehicle_movement()
    return{
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['lat'], location['long']),
        'speed': random.uniform(10, 50),
        'direction': 'North-East',
        'make': 'MERCEDES',
        'model': 'S500',
        'year': 2020,
        'fuel': 'Gasoline',
    }

def generate_gps_data(vehicle_id, timestamp, vihicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'speed': random.uniform(10, 50),
        'direction': 'North-East',
        'vehicleType': vihicle_type
    }

def generate_trafic_camera_data(vehicle_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'cameraId': camera_id,
        'speed': random.uniform(10, 50),
    }

def generate_weather_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(10, 30),
        'humidity': random.uniform(0, 100),
        'weatherCondition': random.choice(['Sunny', 'Rainy', 'Cloudy', 'Snowy']),
        'windSpeed': random.uniform(0, 20),
        'windDirection': random.choice(['North', 'South', 'East', 'West']),
        'airQualityIndex': random.uniform(0, 100),
        'uvIndex': random.uniform(0, 10),
    }
     
def generate_emergency_data(vehicle_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'vehicleId': vehicle_id,
        'timestamp': timestamp,
        'location': location,
        'emergencyType': random.choice(['Accident', 'Breakdown', 'Theft', 'Fire']),
        'description': 'Emergency on the road',
        'status': random.choice(['Active', 'Resolved']),
    }

def json_serializer(data):
    if isinstance(data, uuid.UUID):
        return str(data)
    raise TypeError(f'Object of type {type(data)} is not JSON serializable')

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')

def producer_data_to_kafka(producer, topic, data):
    producer.produce(
        topic=topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()
   

def simulate_journey(producer, vehicle_id):
    while True:
        vehicle_data = generate_vehicle_data(vehicle_id)
        gps_data = generate_gps_data(vehicle_id, vehicle_data['timestamp'])
        trafic_camera_data = generate_trafic_camera_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'], camera_id='Sony-1234')
        weather_data = generate_weather_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_data = generate_emergency_data(vehicle_id, vehicle_data['timestamp'], vehicle_data['location'])

        if(vehicle_data['location'][0] >= BIRMINGHAM_CONORDINATES['lat'] and vehicle_data['location'][1] <= BIRMINGHAM_CONORDINATES['long'] ):
            print('Vehicle has reached the destination. Stopping the simulation.')
            break   

        producer_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        producer_data_to_kafka(producer, GPS_TOPIC, gps_data)
        producer_data_to_kafka(producer, TRAFIC_TOPIC, trafic_camera_data)
        producer_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        producer_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_data)

        time.sleep(5)
        
        


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Error: {err}')
    }
    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'Eric Nguyen')
    except KeyboardInterrupt:
        print('KeyboardInterrupt')
    except Exception as e:
        print(f'Error: {e}')


