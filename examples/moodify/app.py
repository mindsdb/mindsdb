import os
import requests
from flask import Flask, redirect, request, session, jsonify, render_template
from urllib.parse import urlencode
from dotenv import load_dotenv
import google.generativeai as genai  # Gemini SDK
import mindsdb_sdk


load_dotenv()

app = Flask(__name__)

# ===================== CONFIG =====================
SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_REDIRECT_URI = os.getenv("SPOTIFY_REDIRECT_URI")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

app.secret_key = os.getenv("FLASK_SECRET_KEY", "super_secret_key")

# MindsDB Connection
mdb = mindsdb_sdk.connect('http://127.0.0.1:47334')

# ===================== SPOTIFY ENDPOINTS =====================
SPOTIFY_AUTH_URL = "https://accounts.spotify.com/authorize"
SPOTIFY_TOKEN_URL = "https://accounts.spotify.com/api/token"
SPOTIFY_API_BASE_URL = "https://api.spotify.com/v1"

# ===================== GEMINI CONFIG =====================
genai.configure(api_key=GEMINI_API_KEY)
gemini_model = genai.GenerativeModel("gemini-1.5-flash")


# ===================== ROUTES =====================
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/login")
def login():
    scope = "playlist-modify-private playlist-modify-public user-read-email"
    params = {
        "client_id": SPOTIFY_CLIENT_ID,
        "response_type": "code",
        "redirect_uri": SPOTIFY_REDIRECT_URI,
        "scope": scope,
    }
    url = f"{SPOTIFY_AUTH_URL}?{urlencode(params)}"
    return redirect(url)


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if code is None:
        return "Error: No code received", 400

    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": SPOTIFY_REDIRECT_URI,
        "client_id": SPOTIFY_CLIENT_ID,
        "client_secret": SPOTIFY_CLIENT_SECRET,
    }

    response = requests.post(SPOTIFY_TOKEN_URL, data=payload)
    token_info = response.json()

    if "access_token" not in token_info:
        return f"Error: {token_info}", 400

    session["spotify_token"] = token_info["access_token"]

    return redirect("/success")


@app.route("/success")
def success():
    return render_template("index.html", success=True)


@app.route("/status")
def status():
    return jsonify({"logged_in": "spotify_token" in session})


@app.route("/generate", methods=["POST"])
def generate_playlist():
    if "spotify_token" not in session:
        return jsonify({"error": "Not logged in"}), 401

    user_input = request.json.get("mood", "")
    if not user_input.strip():
        return jsonify({"error": "No mood provided"}), 400

    # ===== STEP 1: Gemini classification =====
    try:
        prompt = f"""
        Classify the following text into one of these emotions only:
        [joy, happiness, sadness, anger, fear, surprise, love, disgust, calm, relaxation, optimism, neutral].
        Text: "{user_input}"
        Respond with just the emotion name.
        """
        response = gemini_model.generate_content(prompt)
        mood = response.text.strip().lower()

        valid_emotions = [
            "joy", "happiness", "sadness", "anger", "fear", "surprise",
            "love", "disgust", "calm", "relaxation", "optimism", "neutral"
        ]
        if mood not in valid_emotions:
            mood = "neutral"

    except Exception as e:
        print("Gemini error:", e)
        mood = "neutral"

    # ===== STEP 2: Get genre (MindsDB or fallback) =====
    genre = None
    try:
        query = mdb.sql(f"SELECT genre FROM mood_to_genre_model WHERE mood='{mood}'")
        if query.rows and "genre" in query.rows[0]:
            genre = query.rows[0]["genre"]
    except Exception as e:
        print("MindsDB error:", e)

    if not genre:  # fallback
        mood_to_genre = {
            "joy": "pop",
            "happiness": "dance",
            "sadness": "acoustic",
            "anger": "rock",
            "fear": "metal",
            "surprise": "edm",
            "love": "romance",
            "disgust": "punk",
            "calm": "lofi",
            "relaxation": "chill",
            "optimism": "indie",
            "neutral": "classical",
        }
        genre = mood_to_genre.get(mood, "pop")

    # ===== STEP 3: Get Spotify User ID =====
    headers = {"Authorization": f"Bearer {session['spotify_token']}"}
    profile_resp = requests.get(f"{SPOTIFY_API_BASE_URL}/me", headers=headers)
    user_id = profile_resp.json().get("id")

    # ===== STEP 4: Create Playlist =====
    playlist_name = f"{mood.capitalize()} Vibes 🎶"
    create_payload = {"name": playlist_name, "public": False}
    create_resp = requests.post(
        f"{SPOTIFY_API_BASE_URL}/users/{user_id}/playlists",
        headers=headers,
        json=create_payload,
    )
    playlist = create_resp.json()
    playlist_id = playlist.get("id")

    # ===== STEP 5: Search Songs by Genre =====
    search_resp = requests.get(
        f"{SPOTIFY_API_BASE_URL}/search",
        headers=headers,
        params={"q": genre, "type": "track", "limit": 15},
    )
    tracks = search_resp.json().get("tracks", {}).get("items", [])
    uris = [track["uri"] for track in tracks]

    # ===== STEP 6: Add Songs to Playlist =====
    if uris and playlist_id:
        requests.post(
            f"{SPOTIFY_API_BASE_URL}/playlists/{playlist_id}/tracks",
            headers=headers,
            json={"uris": uris},
        )

    return jsonify({
        "playlist_name": playlist_name,
        "playlist_url": playlist.get("external_urls", {}).get("spotify", ""),
        "mood": mood,
        "genre_used": genre,
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)



