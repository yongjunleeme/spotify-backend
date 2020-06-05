import os
import sys
import boto3
import requests
import base64
import json
import logging
import pymysql
import pandas as pd
import jsonpath
from datetime import datetime

client_id = "83aea29caae84b2e97c1be775f8506b0"
client_secret = "a9ade0f1801f432e951a4d2b70d869af"

host = "spotify-artist.c2i2ypp7xnkb.ap-northeast-2.rds.amazonaws.com"
port = 3306
username = "yongjunlee"
database = "production"
password = "yongjunlee"


def main():

    try:
        conn = pymysql.connect(
            host,
            user=username,
            passwd=password,
            db=database,
            port=port,
            use_unicode=True,
            charset="utf8",
        )
        cursor = conn.cursor()
    except:
        logging.error("could not connect to rds")
        sys.exit(1)

    headers = get_headers(client_id, client_secret)

    cursor.execute("SELECT id FROM artists")

    top_track_keys = {
        "id": "id",
        "name": "name",
        "popularity": "popularity",
        "external_url": "external_urls.spotify",
    }

    dt = datetime.utcnow().strftime("%Y-%m-%d")
    top_tracks = []
    for (id,) in cursor.fetchall():
        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(id)
        params = {"country": "US"}
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)
        for i in raw["tracks"]:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({k: jsonpath.jsonpath(i, v)})
                top_track.update({"artist_id": id})
                top_tracks.append(top_track)

    track_ids = [i["id"][0] for i in top_tracks]
    top_tracks = pd.DataFrame(raw)
    top_tracks.to_parquet("top-tracks.parquet", engine="pyarrow", compression="snappy")
    s3 = boto3.resource("s3")
    object = s3.Object("data-spotify", "top-tracks/dt={}/top-tracks.parquet".format(dt))
    data = open("top-tracks.parquet", "rb")
    object.put(Body=data)

    tracks_batch = [track_ids[i : i + 100] for i in range(0, len(track_ids), 100)]
    audio_features = []
    for i in tracks_batch:
        ids = ",".join(i)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)
        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)
        audio_features.extend(raw["audio_features"])
        print(raw["audio_features"])

    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet(
        "audio-features.parquet", engine="pyarrow", compression="snappy"
    )
    s3 = boto3.resource("s3")
    object = s3.Object(
        "data-spotify", "audio-features/dt={}/audio-features.parquet".format(dt)
    )
    data = open("audio-features.parquet", "rb")
    object.put(Body=data)


def get_headers(client_id, client_secret):
    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode(
        "{}:{}".format(client_id, client_secret).encode("utf-8")
    ).decode("ascii")
    headers = {"Authorization": "Basic {}".format(encoded)}
    payload = {"grant_type": "client_credentials"}
    r = requests.post(endpoint, data=payload, headers=headers)
    access_token = json.loads(r.text)["access_token"]
    headers = {"Authorization": "Bearer {}".format(access_token)}
    return headers


if __name__ == "__main__":
    main()
