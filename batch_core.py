import sys
import requests
import base64
import json
import logging
import pymysql

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
    artists = []

    for (id,) in cursor.fetchall():
        artists.append(id)

    artist_batch = [artists[i : i + 50] for i in range(0, len(artists), 50)]

    for i in artist_batch:
        ids = ",".join(i)
        URL = "https://api.spotify.com/v1/artists/?ids={}".format(ids)
        r = requests.get(URL, headers=headers)
        raw = json.loads(r.text)
        print(raw)


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
