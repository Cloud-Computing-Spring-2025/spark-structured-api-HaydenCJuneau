# Assignment 3 - Music Streams Data Analysis

In this assignment, we use spark to analyze data from a randomly generated music streaming dataset.
The data is generated from `input_generator.py`.
We have six analysis tasks, all relating to different statistics about streams which occurred.

## Datasets
`listening_logs.csv`
---
This dataset contains log data capturing each user's listening activity. Write a script to generate the dataset.
- `user_id` – Unique ID of the user
- `song_id` – Unique ID of the song
- `timestamp` – Date and time when the song was played (e.g., 2025-03-23 14:05:00)
- `duration_sec` – Duration in seconds for which the song was played
---
`songs_metadata.csv`
---
This dataset contains metadata about the songs in the catalog. Write a script to generate the dataset. 
- `song_id` – Unique ID of the song
- `title` – Title of the song
- `artist` – Name of the artist
- `genre` – Genre of the song (e.g., Pop, Rock, Jazz)
- `mood` – Mood category of the song (e.g., Happy, Sad, Energetic, Chill)

## Output
Output for each task is contained in the `output` directory.

## Execution
To execute the scripts, run `spark-submit task_(your task here).py`
