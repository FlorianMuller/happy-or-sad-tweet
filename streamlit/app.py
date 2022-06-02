import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.types import StringType
from dotenv import load_dotenv
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime

load_dotenv()

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("dashboard") \
    .getOrCreate()

# # Read JSON file into dataframe
df = spark.read.json("hdfs://localhost:54310/data/tweets.json")
df = df.withColumn("created_at", regexp_replace('created_at', 'Z','+00:00'))
df.createOrReplaceTempView("Tweets")
general_df = spark.sql("SELECT author_id, created_at, id, lang, text, overall_feeling FROM Tweets")
pd_df = general_df.toPandas()
pd_df['created_at'] = pd_df['created_at'].apply(lambda x: datetime.fromisoformat(x))
pd_df['year'] = pd_df['created_at'].apply(lambda x: x.year)
pd_df['month'] = pd_df['created_at'].apply(lambda x: x.month)
pd_df['day'] = pd_df['created_at'].apply(lambda x: x.day)
pd_df['hour'] = pd_df['created_at'].apply(lambda x: x.hour)
pd_df['minute'] = pd_df['created_at'].apply(lambda x: x.minute)
pd_df['second'] = pd_df['created_at'].apply(lambda x: x.second)

grouped_df = spark.sql("SELECT overall_feeling, COUNT(id) as count FROM Tweets GROUP BY overall_feeling")
pd_grouped_df = grouped_df.toPandas()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
    st.dataframe(pd_df)

    st.header('Regroupement')
    st.dataframe(pd_grouped_df)

    st.header('Répartition')
    fig = plt.figure(figsize=(17,10))
    plt.pie(pd_grouped_df['count'], labels=pd_grouped_df['overall_feeling'], autopct='%.0f%%', colors=['#87E931','#C8DEDF','#F05D3D'])
    st.pyplot(fig)

def page_evolution_temporelle(state):
    st.title("EVOLUTION TEMPORELLE")
    time_scale = st.select_slider('Echelle de temps :', options=['heure', 'minute', 'seconde'])
    if time_scale == "heure":
        df_time = pd_df.groupby(['year', 'month', 'day', 'hour'])['text'].count()
        df_time = df_time.reset_index()
        df_time['time'] = df_time['hour']
    elif time_scale == "minute":
        df_time = pd_df.groupby(['year', 'month', 'day', 'hour', 'minute'])['text'].count()
        df_time = df_time.reset_index()
        df_time['time'] = df_time.apply(lambda row:  str(row.hour) +':'+ str(row.minute), axis = 1)
    else : 
        df_time = pd_df.groupby(['year', 'month', 'day', 'hour', 'minute', 'second'])['text'].count()
        df_time = df_time.reset_index()
        df_time['time'] = df_time.apply(lambda row: str(row.hour) +':'+ str(row.minute) +':'+ str(row.second), axis = 1)
    st.dataframe(df_time)
    fig = plt.figure(figsize=(17,10))
    plt.bar(x=df_time['time'], height=df_time['text'])
    st.pyplot(fig)

def page_tfidf(state):
    st.title("MOTS CLES (TF-IDF)")
    st.write('comming soon')

def page_feed(state):
    st.title("FEED TEMPS REEL")
    st.write('comming soon')

########################################################
# EXECUTION DU CODE
########################################################

if __name__ == '__main__':
    main()

spark.stop()