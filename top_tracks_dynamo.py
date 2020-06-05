import os
import sys
import boto3
import requests
import base64
import json
import logging
import pymysql
import jsonpath

client_id = "83aea29caae84b2e97c1be775f8506b0"
client_secret = "a9ade0f1801f432e951a4d2b70d869af"

host = "spotify-artist.c2i2ypp7xnkb.ap-northeast-2.rds.amazonaws.com"
port = 3306
username = "yongjunlee"
database = "production"
password = "yongjunlee"


def main():
    try:
        dynamodb = boto3.resource(
            "dynamodb",
            region_name="ap-northeast-2",
            endpoint_url="http://dynamodb.ap-northeast-2.amazonaws.com",
        )
    except:
        loggin.error("could not connect to dynamodb")

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
    table = dynamodb.Table("top_tracks")
    cursor.execute("SELECT id FROM artists")
    countries = ["US", "CA"]
    for country in countries:
        for (artist_id,) in cursor.fetchall():
            URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(artist_id)
            params = {"country": "US"}
            r = requests.get(URL, params=params, headers=headers)
            raw = json.loads(r.text)
            for track in raw["tracks"]:
                data = {"artist_id": artist_id, "country": country}
                data.update(track)
                table.put_item(Item=data)
                print(data)


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
