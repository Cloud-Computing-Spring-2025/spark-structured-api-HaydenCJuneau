import csv
import os
import random
from datetime import datetime, timedelta

# Create input directory if it doesn't exist
os.makedirs("input", exist_ok=True)

# Sample song data
song_list = []

song_titles = [
    "NYC", "Lazy Eye", "Melatonin", "Divine Hammer", "Heroes",
    "Waiting for Stevie", "Tangled Dreams", "Fantasy", "Your Face", "Black Metallic"
]
song_artists = [
    "Interpol", "Pearl Jam", "Wisp", "DyE", "Silversun Pickups"
]
song_genres = [
    "Rock", "Pop", "Country", "Shoegaze", "Hip-Hop"
]
song_moods = [
    "Happy", "Sad", "Chill", "Energetic"
]

for i, title in enumerate(song_titles):
    song = {
        "SongID": i + 1,
        "Title": title,
        "Artist": random.choice(song_artists),
        "Genre": random.choice(song_genres),
        "Mood": random.choice(song_moods)
    }
    
    song_list.append(song)

# Write songs.csv
with open("input/songs.csv", mode="w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=song_list[0].keys())
    writer.writeheader()
    writer.writerows(song_list)


# Sample streams
stream_data = []
base_time = datetime.now()

for stream_id in range(101, 301):
    stream = {
        "StreamID": stream_id,
        "UserID": random.randint(100, 110),
        "SongID": random.randint(1, len(song_titles)),
        "Timestamp": (
            (base_time - timedelta(hours=random.randint(0, 240))).strftime("%Y-%m-%d %H:%M:%S")
        ),
        "DurationSec": random.randint(120, 240),
    }
    stream_data.append(stream)

# Write posts.csv
with open("input/streams.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=stream_data[0].keys())
    writer.writeheader()
    writer.writerows(stream_data)

print("✅ Dataset generation complete")
