from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from datetime import datetime
from pymongo import MongoClient
from sklearn.feature_extraction.text import CountVectorizer

import pandas as pd
import json

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""


auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

class Database():
    def __init__(self):
        self.conn = MongoClient('localhost', 27017)
        self.db = self.conn.twitterdb
        self.col = self.db.newtweets

class MyListener(StreamListener):

    def __init__(self):
        self.db = Database()

    def on_data(self, dados):
        tweet = json.loads(dados)
        
        created_at = tweet["created_at"]
        id_str = tweet["id_str"]
        text = tweet["text"]
        obj = {
            "created_at": created_at,
            "id_str": id_str,
            "text": text,
        }

        tweetind = self.db.col.insert_one(obj).inserted_id
        print(obj)
        return True


def analysis():
    db = Database()

    dataset = [{"created_at": item["created_at"], "text": item["text"], } for item in db.col.find()]
    df = pd.DataFrame(dataset)

    cv = CountVectorizer()
    count_matrix = cv.fit_transform(df.text)

    word_count = pd.DataFrame(cv.get_feature_names(), columns=["word"])
    word_count["count"] = count_matrix.sum(axis=0).tolist()[0]
    word_count = word_count.sort_values("count", ascending=False).reset_index(drop=True)
    print(word_count[:50])    



analysis()
# keywords = ['Data Science', 'Machine Learning', 'Naruto']
# mylistener = MyListener()
# mystream = Stream(auth, listener = mylistener)
# mystream.filter(track=keywords)