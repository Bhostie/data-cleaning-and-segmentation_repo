import os
import json
import pandas as pd
import csv
from datetime import datetime


# Read all the json files into a list of dataframes
def read_json_files():

    # Implement the pipeline for a particular user (since I got only one user's text data)
    device_id = "70ff0a1f-a892-49a3-8c54-47dc5f5a1bcb" # Xiaomi Mi 6

    # Read all the json files from the filtered data folder
    data_dir = "filtered_data"
    json_files = [f for f in os.listdir(data_dir) if f.endswith(".json")]
    data_frames = {}
    for file in json_files:
        with open(os.path.join(data_dir, file)) as f:
            print(f"Reading file: {file}")
            json_data = json.load(f)
            file_data = []
            for item in json_data:

                # Convert the timestamp to datetime object
                item["timestamp"] = datetime.fromtimestamp(int(item["timestamp"]) / 1000)

                # Check if the device_id is present in the data
                if "device_id" in item:
                    # Check if the device_id is the one we are looking for
                    if item["device_id"] == device_id:
                        file_data.append(item)
            data_frames[file[0:-5]] = pd.DataFrame(file_data)

    return data_frames

def filter_data(sensor_dict, start_time, end_time):
    filtered_dict = {}
    for key in list(sensor_dict.keys()):
        filtered_dict[key] = sensor_dict[key][(sensor_dict[key]["timestamp"] >= start_time) & (sensor_dict[key]["timestamp"] <= end_time)]
    return filtered_dict

 
# Data Cleaning ####################################################################################################

# Reading the json files into seperate data frames and collecting them into one dictionary
sensor_dict = read_json_files()

# Remove empty dataframes
for key in list(sensor_dict.keys()):
    if sensor_dict[key].empty:
        del sensor_dict[key]

# Ignore duplicates by sorting every sensor data by timestamp
for key in list(sensor_dict.keys()):
    sensor_dict[key] = sensor_dict[key].sort_values(by="timestamp")
    # Drop duplicates
    sensor_dict[key] = sensor_dict[key].drop_duplicates(subset="timestamp")

print("\n\nDataframes:")
print(sensor_dict.keys())
print(sensor_dict["battery_charges"].head())

# Exclude the users with missing sensor data because of their device. (six users)
""" No need for know, since we are only working with one user's data """
discarded_models = ["SM-G610F", "SM-J710FQ", "SM-A710F", "Redmi 6"]

# Read session data from sessions.csv
sessions = pd.read_csv("sessions.csv")

# Convert the start_time and end_time to datetime objects
sessions["start_time"] = pd.to_datetime(sessions["start_time"])
sessions["end_time"] = pd.to_datetime(sessions["end_time"])

# Sort the sessions by start_time
sessions = sessions.sort_values(by="start_time")


print("\n\nSession data types: {}".format(sessions.dtypes))

print("\n\nSensor_Dict data types: {}".format(sensor_dict["battery_charges"].dtypes))

# Based on the sessions, segment the all sensor data into sessions in sensor_dict
# segment_list will contain the segments of each sensor data
segment_list = []

# Traverse every segment interval in sessions
for i in range(len(sessions)):

    # each segment element will contail start_time, end_time, and the dictionary for sensor dataframes
    segment = {}

    # Get the start and end time of the segment
    segment["start_time"] = sessions["start_time"][i]
    segment["end_time"] = sessions["end_time"][i]

    # Filter the sensor data for the segment
    segment["sensor_data"] = filter_data(sensor_dict.copy(), segment["start_time"], segment["end_time"])

    # If the segment is empty, skip it
    if not segment["sensor_data"]:
        continue

    # Append the segment to the list
    segment_list.append(segment)

# Write the segmented data to CSV files
output_dir = "segments"
os.makedirs(output_dir, exist_ok=True)
for i, segment in enumerate(segment_list):
    for key in list(segment["sensor_data"].keys()):
        
        if segment["sensor_data"][key].empty:
            continue
        
        # Create a sub folder for each segment
        os.makedirs(os.path.join(output_dir, f"segment_{i}"), exist_ok=True)

        # Write the sensor data to CSV files
        output_file = os.path.join(output_dir, f"segment_{i}", f"{key}.csv")
        segment["sensor_data"][key].to_csv(output_file, index=False)
        


# Data Segmentation ####################################################################################################
"""
# Window size 2, 5, 10 and 20 seconds with 1 second overlap
window_sizes = [2, 5, 10, 20]
overlap = 1

# Create a new dataframe for each window size
df_windowed = []
for i in range(len(df)):
    for window_size in window_sizes:
        windowed_data = []
        for j in range(0, len(df[i]), window_size - overlap):
            windowed_data.append(df[i].iloc[j:j+window_size])
        df_windowed.append(windowed_data)

print(df_windowed)

# Write windowed data to CSV files
output_dir = "windowed"
os.makedirs(output_dir, exist_ok=True)
for i, windowed_data in enumerate(df_windowed):
    for j, window in enumerate(windowed_data):
        output_file = os.path.join(output_dir, f"window_{i}_{j}.csv")
        window.to_csv(output_file, index=False)

"""

# Feature Extraction ####################################################################################################
