import os
import json
import pandas as pd

# Implement the pipeline for a particular user (since I got only one user's text data)
device_id = "70ff0a1f-a892-49a3-8c54-47dc5f5a1bcb" # Xiaomi Mi 6

# Read all the json files into a list of dataframes
def read_json_files():
    # Read all the json files
    data_dir = "data"
    json_files = [f for f in os.listdir(data_dir) if f.endswith(".json")]
    data_frames = {}
    for file in json_files:
        with open(os.path.join(data_dir, file)) as f:
            print(f"Reading file: {file}")
            json_data = json.load(f)
            file_data = []
            for item in json_data:
                # Check if the device_id is present in the data
                if "device_id" in item:
                    # Check if the device_id is the one we are looking for
                    if item["device_id"] == device_id:
                        file_data.append(item)
            data_frames[file[0:-5]] = pd.DataFrame(file_data)

    return data_frames

sensor_dict = read_json_files()

# save the new dataframes to json files again into folder "filtered_data"

# Create the folder if it doesn't exist
if not os.path.exists("filtered_data"):
    os.makedirs("filtered_data")

for key in list(sensor_dict.keys()):
    sensor_dict[key].to_json(f"filtered_data/{key}.json", orient="records")