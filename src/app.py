import os
from dotenv import load_dotenv
import math
import streamlit as st
import time
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pymongo import MongoClient
from datetime import datetime

load_dotenv()
TOPIC = f"{os.environ['TWITTER_KEYWORD']}_tweet"

client = MongoClient("mongodb://root:example@mongo:27017")

def get_data():
    db = client['ridiculus_elephant']
    items = db[TOPIC].find() 
    items = list(items)
    return items

def create_df_from_list(items):
    author_ids = []
    created_ats = []
    tweet_ids = []
    langs = []
    texts = []
    overall_feelings = []

    # Remove _id
    for d in items:
        d.pop("_id")

    # Print results.
    for item in items:
        author_id = item["author_id"]
        created_at = item["created_at"]
        tweet_id = item["id"]
        lang = item["lang"]
        text = item["text"]
        overall_feeling = item["overall_feeling"]

        author_ids.append(author_id)
        created_ats.append(created_at)
        tweet_ids.append(tweet_id)
        langs.append(lang)
        texts.append(text)
        overall_feelings.append(overall_feeling)

    data = {
        "author_id": author_ids,
        "created_at": created_ats,
        "id": tweet_ids,
        "lang": langs,
        "text": texts,
        "overall_feeling": overall_feelings,
    }
    # Create the pandas DataFrame
    df = pd.DataFrame(data)
    return df

items = get_data()
df = create_df_from_list(items)

# Opération temporelles #
df['created_at'] = df['created_at'].str.replace(r'Z','+00:00')
df['created_at'] = df['created_at'].apply(lambda x: datetime.fromisoformat(x))
df['year'] = df['created_at'].apply(lambda x: x.year)
df['month'] = df['created_at'].apply(lambda x: x.month)
df['day'] = df['created_at'].apply(lambda x: x.day)
df['hour'] = df['created_at'].apply(lambda x: x.hour)
df['minute'] = df['created_at'].apply(lambda x: x.minute)
df['second'] = df['created_at'].apply(lambda x: x.second)

sorted_df = df.sort_values(by=['created_at'], ascending=False)
grouped_df = df.groupby(['overall_feeling'])['overall_feeling'].count()

def main():
    state = st.session_state

    pages = {
        "Nb tweets": page_nb_tweet,
        "Evolution temporelle": page_evolution_temporelle,
        "Mots clés (TF-IDF)": page_tfidf,
        "Feed temps réel": page_feed
    }

    st.sidebar.title("Projet Big Data 2")
    st.sidebar.subheader("Menu")
    selection = st.sidebar.radio("", tuple(pages.keys()))
    pages[selection](state)

def page_nb_tweet(state):
    st.title("NB TWEETS")
    st.header('Général')
    st.dataframe(df)

    st.header('Regroupement')
    st.dataframe(grouped_df)

    st.header('Répartition')
    fig = plt.figure(figsize=(17,10))
    plt.pie(grouped_df.values, labels=grouped_df.index, autopct='%.0f%%', colors=['#F05D3D','#C8DEDF','#87E931'])
    st.pyplot(fig)

def page_evolution_temporelle(state):
    st.title("EVOLUTION TEMPORELLE")
    time_scale = st.select_slider('Echelle de temps :', options=['heure', 'minute', 'seconde'])
    if time_scale == "heure":
        df_time = df.groupby(['year', 'month', 'day', 'hour'])['text'].count()
        df_time = df_time.reset_index()
        df_time['time'] = df_time['hour']
    elif time_scale == "minute":
        df_time = df.groupby(['year', 'month', 'day', 'hour', 'minute'])['text'].count()
        df_time = df_time.reset_index()
        df_time['time'] = df_time.apply(lambda row:  str(row.hour) +':'+ str(row.minute), axis = 1)
    else : 
        df_time = df.groupby(['year', 'month', 'day', 'hour', 'minute', 'second'])['text'].count()
        df_time = df_time.reset_index()
        df_time['time'] = df_time.apply(lambda row: str(row.hour) +':'+ str(row.minute) +':'+ str(row.second), axis = 1)
    st.dataframe(df_time)
    fig = plt.figure(figsize=(17,10))
    plt.bar(x=df_time['time'], height=df_time['text'])
    st.pyplot(fig)


def page_tfidf(state):
    st.title("MOTS CLES (TF-IDF)")

    df['word'] = df['text'].apply(lambda x: x.split(" "))
    df_word = df[["word", "overall_feeling"]]
    df_word = df_word.explode('word')
    df_word = df_word[df_word['word'].str.contains("http") == False]
    #st.dataframe(df_word)

    word_tf = df_word.groupby(['overall_feeling', 'word'])['word'].count()
    word_tf.rename("tf", inplace = True)
    word_tf = word_tf.reset_index()
    #st.dataframe(word_tf)

    word_df = df_word.groupby(['word'])['overall_feeling'].nunique()
    word_df.rename("df", inplace = True)
    word_df = word_df.reset_index()
    #st.dataframe(word_df)

    tf_idf = pd.merge(word_tf, word_df, how="left", on=["word"])
    tf_idf['df'] = tf_idf['df'].apply(lambda x: math.log(3 / x))
    tf_idf['tf_idf'] = tf_idf['tf'] * tf_idf['df']
    #st.dataframe(tf_idf)
    
    st.markdown("## Most representative words")

    feelings = ["Positive", "Negative", "Neutral"]
    for feel in feelings:
        st.markdown(f"### For {feel.lower()} tweets")
        df_to_show = tf_idf[tf_idf["overall_feeling"] == feel]
        df_to_show = df_to_show[['word', 'tf_idf', 'tf']]
        df_to_show.sort_values(by=['tf_idf'], ascending=False, inplace=True)
        st.dataframe(df_to_show, width=500)

    # method
    with st.expander("What's TF-IDF ?"):
        st.markdown("tf–idf, short for \"term frequency–inverse document frequency\", is a numerical statistic that is intended to reflect how important a word is to a document in a collection or corpus")
        st.image("https://www.seoquantum.com/sites/default/files/tf-idf-2-1-1024x375.png")

def page_feed(state):
    st.title("FEED TEMPS REEL")
    # creating a single-element container.
    placeholder = st.empty()

    old_tweet_count = None
    while True:
        items = get_data()
        df = create_df_from_list(items)
        with placeholder.container():
            # Tweet count
            tweet_count = df['id'].count()
            st.metric("Number of tweets", tweet_count, int(tweet_count - (old_tweet_count or tweet_count)))
            old_tweet_count = tweet_count
            
            st.markdown("### Last tweet")
            st.dataframe(sorted_df)

        time.sleep(1)

########################################################
# EXECUTION DU CODE
########################################################

if __name__ == '__main__':
    main()