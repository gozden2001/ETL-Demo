import sqlalchemy
import pandas as pd # type: ignore
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
import requests
import json
from datetime import datetime
import datetime

DATABASE_LOCATION = "mysql://root:root@localhost/etldemo_db"
USER_ID = "gozden01"

#transform
def check_if_valid_data(df: pd.DataFrame) -> bool:
    if df.empty:
        print("No songs downloaded. Finishing execution")
        return False
    
    if pd.Series(df['played_at']).is_unique:
        pass
    else:
        raise Exception("Primary key Check is violated")
    
    if df.isnull().values.any():
        raise Exception("Empty rows!!!!!!!!")
    
    # yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    # yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # timestamps = df['timestamp'].tolist()
    # for timestamp in timestamps:
    #     if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday:
    #         raise Exception("At least one of the returned songs does not come from within the last 24 hours")
    
    return True

if __name__ == "__main__":

    headers = {
        "Accept" : "application/json",
        "Content-Type" : "application/json",
        "Authorization" : "Bearer BQD_lNKXgmHmOhqXRIcpjZDGhHa43Tv9jIDh4f5QEqdzVYWm8PouIrDf-6oH1mfCAUvcAI0y5Lvy0ubb_ffp3BHEwoqYAgPGYZP9i_VY38HLYmyuX3vTyYW0SV8BwYCZLxGcx1gzee1bBJels5opuZu5eRxy1zraiAzCS0T4WZnGwivc55HeB1lEDMw7CUwoaNJnUnCEWj8"
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    r = requests.get("https://api.spotify.com/v1/me/player/recently-played?after={time}".format(time=yesterday_unix_timestamp), headers=headers)

    data = r.json()
    #print(data)

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    song_dict = {
        "song_name" : song_names,
        "artist_name" : artist_names,
        "played_at" : played_at_list,
        "timestamp" : timestamps
    }

    #Validate
    song_df = pd.DataFrame(song_dict, columns= ["song_name", "artist_name", "played_at", "timestamp"])
    if check_if_valid_data(song_df):
        print(song_df)

    #Load
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)

    sql_query_string = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """
    sql_query = text(sql_query_string)

    with engine.connect() as connection:
        connection.execute(sql_query)
        print("Table 'my_played_tracks' created successfully")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists='append')
    except:
        print("Data already exists in the database")

    print("Database closed successfully")

