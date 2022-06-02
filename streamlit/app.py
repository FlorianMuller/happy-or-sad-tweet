import os
import time
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
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

def get_json_from_dfs(file_path):
    return spark.read.json(f"hdfs://localhost:54310{file_path}")

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
    df = get_json_from_dfs("/data/tweets.json")

    # Splitting twwet in words
    words = df \
        .withColumn("word", f.explode(f.split("text", "\s+"))) \
        .select("word", "overall_feeling") \
        .filter(f.col("word").contains("http") == False)

    word_tf = words.groupBy("overall_feeling", "word").agg(f.count("word").alias("term_frequency")).alias("tf")
    word_df = words.distinct().groupBy("word").agg(f.count("overall_feeling").alias("document_frequency")).alias("df")
    tf_idf = word_tf \
        .join(word_df, word_tf.word == word_df.word, "left") \
        .withColumn("tf_idf", f.col("term_frequency") * f.log(3 / f.col("document_frequency")))
    
    st.markdown("## Most representative words")

    feelings = ["Positive", "Negative", "Neutral"]
    for feel in feelings:
        st.markdown(f"### For {feel.lower()} tweets")
        st.dataframe(tf_idf \
            .filter(tf_idf["overall_feeling"] == feel) \
            .select("tf.word", "tf_idf", "term_frequency") \
            .sort(f.col("tf_idf").desc()) \
            .toPandas(),    
        width=500)




def page_feed(state):
    st.title("FEED TEMPS REEL")
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
            
            st.markdown("### Last tweet")
            st.dataframe(df \
                .sort(f.col("created_at").desc()) \
                .select("text") \
                .toPandas()
            )

        time.sleep(1)

def page_equipe(state):
    st.title("LES BOSS")

########################################################   
# EXECUTION DU CODE
########################################################

if __name__ == '__main__':
    main()

spark.stop()