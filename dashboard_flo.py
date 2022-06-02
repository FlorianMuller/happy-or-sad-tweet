import time
from dotenv import load_dotenv
import streamlit as st
import streamlit.components.v1 as components
from pyspark.sql import SparkSession

load_dotenv()

st.set_page_config(
    page_title="ukraine tweets Dashboard",
    page_icon="🇺🇦",
    # layout="wide",
)

st.title("Tweets about Ukraine 🇺🇦")

def get_json_from_dfs(file_path):
    return spark.read.json(f"hdfs://localhost:54310{file_path}")


# creating a single-element container.
placeholder = st.empty()
spark = SparkSession.builder.getOrCreate()



old_tweet_count = None
while True:
    df = get_json_from_dfs("/data/tweets.json")

    with placeholder.container():

        # Tweet count
        tweet_count = df.count()
        st.metric("Number of tweets", tweet_count, tweet_count - (old_tweet_count or tweet_count))
        old_tweet_count = tweet_count
        
        st.markdown("### Detailed Data View")
        st.dataframe(df.select("text", "created_at").toPandas())

    time.sleep(1)
