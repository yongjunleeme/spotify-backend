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
    params = {"q": "bts", "type": "artist", "limit": "1"}

    r = requests.get(
        "https://api.spotify.com/v1/search", params=params, headers=headers
    )
    raw = json.loads(r.text)

    artist = {}
    artist_raw = raw["artists"]["items"][0]
    if artist_raw["name"] == params["q"]:
        artist.update(
            {
                "id": artist_raw["id"],
                "name": artist_raw["name"],
                "followers": artist_raw["followers"]["total"],
                "popularity": artist_raw["popularity"],
                "url": artist_raw["external_urls"]["spotify"],
                "image_url": artist_raw["images"][0]["url"],
            }
        )
    insert_row(cursor, artist, "artists")
    conn.commit()


def insert_row(cursor, data, table):
    placeholders = ", ".join(["%s"] * len(data))
    columns = ", ".join(data.keys())
    key_placeholders = ", ".join(["{0}=%s".format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (
        table,
        columns,
        placeholders,
        key_placeholders,
    )
    cursor.execute(sql, list(data.values()) * 2)


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
