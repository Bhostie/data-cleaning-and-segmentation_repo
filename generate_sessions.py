import pandas as pd

# Read keyboard.json file into a dataframe
df = pd.read_json('keyboard.json')

# Session Detection based on text input and timestamps
# Create a new column to store the time difference between two consecutive keypresses
df['time_diff'] = df['timestamp'].diff()

#Duplicate timestamp column as ts
df['ts'] = df['timestamp']

# Convert timestamp to datetime object
df['ts'] = pd.to_datetime(df['ts'])

# Sort the data by timestamp, just in case it's not already sorted
df = df.sort_values(by='ts')

# Define the session timeout threshold
threshold = 1 # in minutes
session_timeout = pd.Timedelta(minutes=threshold)

# Initialize session ID
session_id = 0
df['session_id'] = session_id

# Iterate over the rows and assign session IDs
for i in range(1, len(df)):
    time_diff = df['ts'].iloc[i] - df['ts'].iloc[i - 1]
    
    if time_diff > session_timeout:
        # If the gap is greater than the session timeout, start a new session
        session_id += 1
        print(f"New session started: {session_id}")
    

    df.at[i, 'session_id'] = session_id


print(df['session_id'].value_counts())
print("\n\n\nCLEANED DATA")

# Data Cleaning - Remove sessions with less than 5 keypresses
session_counts = df['session_id'].value_counts()
valid_sessions = session_counts[session_counts >= 5].index
df = df[df['session_id'].isin(valid_sessions)]

# Reassign session IDs to start from 0
df['session_id'] = pd.Categorical(df['session_id'])
df['session_id'] = df['session_id'].cat.codes

# Display session counts
print(df['session_id'].value_counts())


# Now, let's calculate session start and end times, duration, and number of keypresses for each session.
# Group the data by session_id
grouped = df.groupby('session_id')

# Calculate session start and end times
session_start = grouped['timestamp'].min()
session_end = grouped['timestamp'].max()


start_ts = grouped['ts'].min()
end_ts = grouped['ts'].max()

# Calculate session duration
session_duration = session_end - session_start

# Calculate number of keypresses in each session
keypresses = grouped.size()

# Create a new DataFrame to store session information
session_info = pd.DataFrame({
    'start_time': session_start,
    'end_time': session_end,
    'duration': session_duration,
    'keypresses': keypresses,
})

# Save the session information to a CSV file
session_info.to_csv('sessions.csv')