import time
import random
import json
import datetime
from confluent_kafka import Producer

# Define the set of actions and pages
date_stamp = datetime.datetime(2023, 1, 1, 0, 0)
temperature_value = 2
humidity_value = 75
uv_value = 0

# Kafka configuration
def get_current_directory():
  import os
  cwd = os.getcwd()
  print(f"Current working directory: {cwd}")
  return cwd

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

# Data Generation
def generate_log_entry():
    global date_stamp, temperature_value, humidity_value, uv_value  
    start_night = datetime.time(19, 0)  
    end_night = datetime.time(7, 0)    
    
    #values generation, first night
    if date_stamp.time() >= start_night or date_stamp.time() < end_night:   #night
        #uv
        uv_value = 0

        #temperature
        perturbation_temp = int(random.uniform(-5, 1))
        new_temperature_value = temperature_value + perturbation_temp
        temperature_value = max (-5, min(6, new_temperature_value))

        #humidity
        perturbation_humidity = int(random.uniform(-5, 5))
        new_humidity_value = humidity_value + perturbation_humidity
        humidity_value = max (50, min(90, new_humidity_value))

    else:      #day                                                 
        #uv and temperature, we seperate the day in two parts: before 2am they rise and after they progressivly fall 
        if date_stamp.time() <= datetime.time(14,0):
            uv_value += 1 if random.random() < 0.4 else 0      #40% chance to rise so it doesnt rise too fast

            perturbation_temp = int(random.uniform(-1, 4))
            new_temperature_value = temperature_value + perturbation_temp
            temperature_value = max (2, min(12, new_temperature_value))
        else:
            new_uv_value = uv_value - uv_value/ 7                #so it declines progressivly towards 0
            uv_value = new_uv_value

            new_temp_value = temperature_value - temperature_value / 7
            temperature_value = new_temp_value

        #humidity
        perturbation_humidity = int(random.uniform(-5, 5))
        new_humidity_value = humidity_value + perturbation_humidity
        humidity_value = max (5, min(40, new_humidity_value))

    log_entry = {
       "datestamp": date_stamp.isoformat(),
       "temperature": int(temperature_value),
        "humidity": humidity_value,
        "uv": int(uv_value),
    }
    
    date_stamp += datetime.timedelta(hours=1)

    return log_entry
"""
for i in range(24):
    print(generate_log_entry())
"""

def produce(topic, config):
  # creates a new producer instance
  producer = Producer(config)

  while True:
    log_entry = generate_log_entry()
    # Convert log entry to JSON format
    log_entry_json = json.dumps(log_entry)
        
    # Produce message to Kafka
    producer.produce(topic, key=log_entry["datestamp"], value=log_entry_json)
    print(f"Produced message to topic {topic}: {log_entry_json}")

    # send any outstanding or buffered messages to the Kafka broker
    producer.flush()

    # Random delay between 0.1 and 1 second to simulate 1-10 messages per second
    time.sleep(random.uniform(0.1, 1.0))

def main():
  import os
  cwd = os.getcwd()
  print(f"Current working directory: {cwd}")
  config = read_config()
  topic = "temperature"
  produce(topic, config)


main()
