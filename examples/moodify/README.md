🎶 Moodify – AI-Powered Playlist Generator

Moodify is an AI-powered web app that generates Spotify playlists based on your mood.
It uses Google Gemini AI to classify emotions from text, MindsDB to map moods to genres, and the Spotify Web API to create personalized playlists in real time.

✨ Features

🔑 Spotify OAuth Login – securely connect your Spotify account

🧠 Gemini Integration – classifies user mood into one of 12 emotions

🎶 MindsDB-powered Recommendations – maps detected mood → music genre

📀 Spotify Playlist Generator – automatically creates a playlist and fills it with songs

⚙️ Tech Stack

Backend: Flask (Python)

Frontend: HTML + TailwindCSS

AI/ML: Google Gemini API, MindsDB

Music Data: Spotify Web API

🚀 How It Works

User enters how they feel → “I’m stressed but hopeful.”

Gemini API → classifies text into an emotion (e.g., relaxation).

MindsDB → maps mood to a genre (e.g., chill).

Spotify API →

Creates a private playlist for the user

Fetches songs matching that genre

Populates the playlist automatically

🎧 User gets a clickable Spotify playlist link!

🛠️ Setup Instructions
1. Clone the Repo
git clone https://github.com/mindsdb/mindsdb.git
cd mindsdb/examples/moodify

2. Install Requirements
pip install -r requirements.txt

3. Environment Variables

Create a .env file (you can copy from .env.template) and add:

SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
SPOTIFY_REDIRECT_URI=http://127.0.0.1:5000/callback

GOOGLE_API_KEY=your_gemini_api_key
MINDSDB_HOST=http://127.0.0.1:47334
FLASK_SECRET_KEY=some_secret_key

4. Run Flask App
python app.py


The app will be available at:
👉 http://127.0.0.1:5000

📌 Example Usage

Input:

I’m feeling stressed but also a little hopeful today.


Output:

Gemini → relaxation

MindsDB → chill

Spotify → Playlist created with Chill tracks 🎶

🤝 Contributing

This is an example app built for MindsDB Open Source Contributions.
Feel free to improve it by:

Adding more moods / genres mapping

Enhancing UI with more personalization

Expanding playlist length and filtering