import time
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as f
from pyspark.sql.functions import regexp_replace
from dotenv import load_dotenv
import streamlit as st
import plotly.express as px
import plotly.graph_objects as pgo
import matplotlib.pyplot as plt
#import seaborn as sns
from datetime import datetime

load_dotenv()

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("dashboard") \
    .getOrCreate()

# Read JSON file into dataframe
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

def get_json_from_dfs(file_path):
    #return spark.read.json(f"hdfs://localhost:54310{file_path}")
    return spark.read.json(f"hdfs://localhost:9000{file_path}")


#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def main():
    state = st.session_state

    pages = {
        "Nb tweets": page_nb_tweet,
        "Evolution temporelle": page_evolution_temporelle,
        "Mots clés (TF-IDF)": page_tfidf,
        "Feed temps réel": page_feed,
        "Versus Analyzers": page_NLP_compare
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

    # method
    with st.expander("What's TF-IDF ?"):
        st.markdown("tf–idf, short for \"term frequency–inverse document frequency\", is a numerical statistic that is intended to reflect how important a word is to a document in a collection or corpus")
        st.image("https://www.seoquantum.com/sites/default/files/tf-idf-2-1-1024x375.png")


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

def page_NLP_compare(state):
    st.title("Versus Analyzers")
    # creating a single-element container.
    placeholder = st.empty()
    spark = SparkSession.builder.getOrCreate()
    #fig_col1 = st.columns(1)[0]
    
    #for seconds in range(200):
    while True:
      df = get_json_from_dfs("/data/tweets.json")
      with placeholder.container():
      #with fig_col1:
        #df_comparison_per_tweets = df.select("overall_feeling","afinn_feeling","spacy_feeling").groupby("overall_feeling","afinn_feeling","spacy_feeling").agg(f.count("overall_feeling").alias("VaderCount"),f.count("afinn_feeling").alias("AfinnCount"),f.count("spacy_feeling").alias("SpacyCount"))

        # Kept for easy access needs
        feelings = {} 
        feelings["afinn_positive"] = df.where(df.afinn_feeling == "Positive").count()
        feelings["afinn_neutral"] = df.where(df.afinn_feeling == "Neutral").count()
        feelings["afinn_negative"] = df.where(df.afinn_feeling == "Negative").count()
        feelings["spacy_positive"] = df.where(df.spacy_feeling == "Positive").count()
        feelings["spacy_neutral"] = df.where(df.spacy_feeling == "Neutral").count()
        feelings["spacy_negative"] = df.where(df.spacy_feeling == "Negative").count()
        feelings["vader_positive"] = df.where(df.overall_feeling == "Positive").count()
        feelings["vader_neutral"] = df.where(df.overall_feeling == "Neutral").count()
        feelings["vader_negative"] = df.where(df.overall_feeling == "Negative").count()
        #print(feelings)

        # To format in DataFrame to use in Histogram (bar)
        df_feels = spark.createDataFrame([
              Row(analyzer="Spacy",sentiment="Positive",count=feelings["spacy_positive"]),
              Row(analyzer="Spacy",sentiment="Neutral",count=feelings["spacy_neutral"]),
              Row(analyzer="Spacy",sentiment="Negative",count=feelings["spacy_negative"]),
              Row(analyzer="Afinn",sentiment="Positive",count=feelings["afinn_positive"]),
              Row(analyzer="Afinn",sentiment="Neutral",count=feelings["afinn_neutral"]),
              Row(analyzer="Afinn",sentiment="Negative",count=feelings["afinn_negative"]),
              Row(analyzer="Vader",sentiment="Positive",count=feelings["vader_positive"]),
              Row(analyzer="Vader",sentiment="Neutral",count=feelings["vader_neutral"]),
              Row(analyzer="Vader",sentiment="Negative",count=feelings["vader_negative"])
        ])

        # Subtitle
        st.markdown("NLP engines Comparison")
        
        #analyzers = ["Spacy","Spacy","Spacy","Afinn","Afinn","Afinn","Vader","Vader","Vader"]
        #Format du style sentiment = ["Positive","Positive","Positive","Neutral","Neutral","Neutral","Negative","Negative","Negative"]
        
        #fig = px.histogram(dataframe=feelings, x="Analyzer", color="Feeling",category_orders=dict(["Spacy", "Afinn", "Vader"]))
        fig = px.bar(df_feels.toPandas(),x="analyzer",y="count",color="sentiment",barmode="group")
        #fig.show()
        #fig = pgo.Figure()
        #df_feels.show()
        #for analyzer, sentiments in df_feels.collect(): 
        #  fig.add_trace(pgo.Bar(x=analyzer,y=count_feelings)
        
        st.write(fig)
      time.sleep(1)
    
 

########################################################
# EXECUTION DU CODE
########################################################

if __name__ == '__main__':
    main()

spark.stop()
