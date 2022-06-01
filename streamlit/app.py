import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import streamlit as st
import matplotlib.pyplot as plt 
import seaborn as sns
#import app_session as session

load_dotenv()

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("dashboard") \
    .getOrCreate()

# # Read JSON file into dataframe    
df = spark.read.json("hdfs://localhost:54310/data/tweets.json")
df.createOrReplaceTempView("Tweets")
general_df = spark.sql("SELECT text , overall_feeling FROM Tweets")
#print(general_df.show())
pd_general_df = general_df.toPandas()

grouped_df = spark.sql("SELECT overall_feeling, COUNT(id) as count FROM Tweets GROUP BY overall_feeling")
pd_grouped_df = grouped_df.toPandas()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():
    state = st.session_state
    
    pages = {
        "Nb tweets": page_nb_tweet,
        "Evolution temporelle": page_evolution_temporelle,
        "Mots clés (TF-IDF)": page_tfidf,
        "Feed temps réel": page_feed,
        "A propos de l'équipe": page_equipe,
    }
    
    st.sidebar.title("Projet Big Data 2")
    st.sidebar.subheader("Menu") 
    selection = st.sidebar.radio("", tuple(pages.keys()))
    pages[selection](state)

def page_nb_tweet(state):
    st.title("NB TWEETS")
    st.header('Général')
    st.dataframe(pd_general_df)

    st.header('Regroupement')
    st.dataframe(pd_grouped_df)

    st.header('Répartition')
    fig = plt.figure(figsize=(17,10))
    sns.barplot(data=pd_grouped_df, x=pd_grouped_df['overall_feeling'], y=pd_grouped_df['count'])
    st.pyplot(fig)

def page_evolution_temporelle(state):
    st.title("EVOLUTION TEMPORELLE")
    st.write('comming soon')

def page_tfidf(state):
    st.title("MOTS CLES (TF-IDF)")
    st.write('comming soon')

def page_feed(state):
    st.title("FEED TEMPS REEL")
    st.write('comming soon')

def page_equipe(state):
    st.title("LES BOSS")

########################################################   
# EXECUTION DU CODE
########################################################

if __name__ == '__main__':
    main()

spark.stop()