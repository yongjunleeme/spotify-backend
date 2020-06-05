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

    album_keys = {"name": "name", "images": "images"}

    dt = datetime.utcnow().strftime("%Y-%m-%d")
    albums = []
    for (id,) in cursor.fetchall():
        URL = "https://api.spotify.com/v1/artists/{}/albums".format(id)
        params = {"country": "US", "limit": 50}
        r = requests.get(URL, params=params, headers=headers)
        raw = json.loads(r.text)
        for i in raw["items"][0]:
            album = {}
            for k, v in album_keys.items():
                album.update({k: jsonpath.jsonpath(i, v)})
                album.update({"artist_id": id})
                albums.append(album)

    albums = pd.DataFrame(raw)
    albums.to_parquet("albums.parquet", engine="pyarrow", compression="snappy")
    s3 = boto3.resource("s3")
    object = s3.Object("data-spotify", "albums/dt={}/albums.parquet".format(dt))
    data = open("albums.parquet", "rb")
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
