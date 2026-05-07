import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.streaming import DataStreamReader, StreamingQuery
from random import random, randint
from pyspark.sql.types import *

def get_spotify_stream(session: SparkSession) -> DataFrame:
    # Lista piosenek z "szansą" na wylosowanie (wagi), żeby symulować hity
    # Format: (Tytuł, Gatunek, Waga_popularności)
    songs_data = [
        ("Blinding Lights", "Pop", 0.3),       # Hit
        ("Shape of You", "Pop", 0.2),          # Hit
        ("Bohemian Rhapsody", "Rock", 0.1),
        ("Smells Like Teen Spirit", "Rock", 0.1),
        ("Take Five", "Jazz", 0.05),
        ("So What", "Jazz", 0.05),
        ("Despacito", "Latino", 0.1),
        ("Rolling in the Deep", "Pop", 0.1)
    ]
    
    # Tworzymy mały DataFrame pomocniczy z piosenkami
    songs_df = session.createDataFrame(songs_data, ["title", "genre", "weight"])

    # Generujemy strumień zdarzeń (użytkownik klika "Play")
    stream_df = (
        session
        .readStream
        .format("rate")
        .option("rowsPerSecond", "10")  # 10 odtworzeń na sekundę
        .load()
        .withColumn("user_id", (F.rand() * 1000).cast("int")) # Losowy user ID
        .withColumn("rand_val", F.rand()) # Losowa wartość do wyboru piosenki
    )

    # Dołączamy piosenki do strumienia (prostym joinem, w streamingu to tzw. stream-static join)
    # Uwaga: W prostym demo można to zrobić też przez CASE WHEN, co jest bezpieczniejsze dla rate source
    
    # Wersja uproszczona bez joina (działa stabilniej w prostych demach rate-stream):
    music_stream = (
        stream_df
        .withColumn("song_info", 
            F.when(F.col("rand_val") < 0.3, F.array(F.lit("Blinding Lights"), F.lit("Pop")))
            .when(F.col("rand_val") < 0.5, F.array(F.lit("Shape of You"), F.lit("Pop")))
            .when(F.col("rand_val") < 0.6, F.array(F.lit("Bohemian Rhapsody"), F.lit("Rock")))
            .when(F.col("rand_val") < 0.7, F.array(F.lit("Smells Like Teen Spirit"), F.lit("Rock")))
            .when(F.col("rand_val") < 0.8, F.array(F.lit("Despacito"), F.lit("Latino")))
            .otherwise(F.array(F.lit("Take Five"), F.lit("Jazz")))
        )
        .select(
            F.col("timestamp"),
            F.col("user_id"),
            F.col("song_info")[0].alias("title"),
            F.col("song_info")[1].alias("genre")
        )
    )
    
    return music_stream