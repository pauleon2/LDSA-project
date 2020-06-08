import h5py
import sys
import os
import glob
import csv
import pyspark
from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from hdfs import InsecureClient
import numpy as np
import tables
import time
import string
from sys import getsizeof



# Set up the spark application
def setup():
    # calculate the number of partitions we want for the rdds
    num_nodes = 3
    num_rep = 2 * num_nodes
    #sc.stop()
    conf = (SparkConf()
    .setMaster("spark://192.168.2.110:7077")
    .setAppName("Group14")
    .set("spark.executor.cores", 2) # if anything >2, does not run
    .set("spark.pytspark.python","python3")
    .set("spark.dynamicAllocation.enabled", False)
    .set("spark.shuffle.service.enabled", False)
    .set("spark.executor.memory", "2g")
    .set("spark.local.dir", "/home/ubuntu/MillionSong/spark/tmp"))

    #sc = SparkContext(conf = conf)
    sc = SparkContext.getOrCreate(conf)

          



def get_title(file, idx=0):
    return file['metadata']['songs']['title'][idx].decode("utf-8")

def get_artist_name(file, idx=0):
    return file['metadata']['songs']['artist_name'][idx].decode("utf-8")


def get_artist_familiarity(h5,songidx=0):
    """
    Get artist familiarity from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['artist_familiarity'][songidx]).encode('utf-8', 'ignore').decode("utf-8")

def get_artist_hotttnesss(h5,songidx=0):
    """
    Get artist hotttnesss from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['artist_hotttnesss'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_artist_id(h5,songidx=0):
    """
    Get artist id from a HDF5 song file, by default the first song in it
    """
    return h5['metadata']['songs']['artist_id'][songidx].decode("utf-8")

def get_artist_mbid(h5,songidx=0):
    """
    Get artist musibrainz id from a HDF5 song file, by default the first song in it
    """
    return h5['metadata']['songs']['artist_mbid'][songidx].decode("utf-8")

def get_artist_playmeid(h5,songidx=0):
    """
    Get artist playme id from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['artist_playmeid'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_artist_7digitalid(h5,songidx=0):
    """
    Get artist 7digital id from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['artist_7digitalid'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_artist_latitude(h5,songidx=0):
    """
    Get artist latitude from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['artist_latitude'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_artist_longitude(h5,songidx=0):
    """
    Get artist longitude from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['artist_longitude'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_artist_location(h5,songidx=0):
    """
    Get artist location from a HDF5 song file, by default the first song in it
    """
    return h5['metadata']['songs']['artist_location'][songidx].decode("utf-8")


def get_release(h5,songidx=0):
    """
    Get release from a HDF5 song file, by default the first song in it
    """
    return h5['metadata']['songs']['release'][songidx].decode("utf-8")

def get_release_7digitalid(h5,songidx=0):
    """
    Get release 7digital id from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['release_7digitalid'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_song_id(h5,songidx=0):
    """
    Get song id from a HDF5 song file, by default the first song in it
    """
    return h5['metadata']['songs']['song_id'][songidx].decode("utf-8")

def get_song_hotttnesss(h5,songidx=0):
    """
    Get song hotttnesss from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['song_hotttnesss'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_track_7digitalid(h5,songidx=0):
    """
    Get track 7digital id from a HDF5 song file, by default the first song in it
    """
    return str(h5['metadata']['songs']['track_7digitalid'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_analysis_sample_rate(h5,songidx=0):
    """
    Get analysis sample rate from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['analysis_sample_rate'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_audio_md5(h5,songidx=0):
    """
    Get audio MD5 from a HDF5 song file, by default the first song in it
    """
    return h5['analysis']['songs']['audio_md5'][songidx].decode("utf-8")


def get_danceability(h5,songidx=0):
    """
    Get danceability from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['danceability'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_duration(h5,songidx=0):
    """
    Get duration from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['duration'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_end_of_fade_in(h5,songidx=0):
    """
    Get end of fade in from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['end_of_fade_in'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_energy(h5,songidx=0):
    """
    Get energy from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['energy'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_key(h5,songidx=0):
    """
    Get key from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['key'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_key_confidence(h5,songidx=0):
    """
    Get key confidence from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['key_confidence'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_loudness(h5,songidx=0):
    """
    Get loudness from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['loudness'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_mode(h5,songidx=0):
    """
    Get mode from a HDF5 song file, by default the first song ifn it
    """
    return str(h5['analysis']['songs']['mode'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_mode_confidence(h5,songidx=0):
    """
    Get mode confidence from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['mode_confidence'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_start_of_fade_out(h5,songidx=0):
    """
    Get start of fade out from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['start_of_fade_out'][songidx]).encode('utf-8','ignore').decode("utf-8")


def get_tempo(h5,songidx=0):
    """
    Get tempo from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['tempo'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_time_signature(h5,songidx=0):
    """
    Get signature from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['time_signature'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_time_signature_confidence(h5,songidx=0):
    """
    Get signature confidence from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['time_signature_confidence'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_track_id(h5,songidx=0):
    """
    Get track id from a HDF5 song file, by default the first song in it
    """
    return str(h5['analysis']['songs']['track_id'][songidx]).encode('utf-8','ignore').decode("utf-8")

def get_year(h5,songidx=0):
    """
    Get release year from a HDF5 song file, by default the first song in it
    """
    return str(h5['musicbrainz']['songs']['year'][songidx]).encode('utf-8','ignore').decode("utf-8")


def read(x):
    import io
    import h5py
    with h5py.File(io.BytesIO(x), 'r') as f:
        song = []     
        song.append(get_artist_name(f))
        song.append(get_title(f))
        song.append(get_artist_familiarity(f))
        song.append(get_artist_hotttnesss(f))
        song.append(get_artist_id(f))
        song.append(get_artist_mbid(f))
        song.append(get_artist_playmeid(f))
        song.append(get_artist_7digitalid(f))
        song.append(get_artist_latitude(f))
        song.append(get_artist_longitude(f))
        song.append(get_artist_location(f))
        song.append(get_release(f))
        song.append(get_release_7digitalid(f))
        song.append(get_song_id(f))
        song.append(get_song_hotttnesss(f))
        song.append(get_track_7digitalid(f))
        song.append(get_analysis_sample_rate(f))
        song.append(get_audio_md5(f))
        song.append(get_danceability(f))
        song.append(get_duration(f))
        song.append(get_end_of_fade_in(f))
        song.append(get_energy(f))
        song.append(get_key(f))
        song.append(get_key_confidence(f))
        song.append(get_loudness(f))
        song.append(get_mode(f))
        song.append(get_mode_confidence(f))
        song.append(get_start_of_fade_out(f))
        song.append(get_tempo(f))
        song.append(get_time_signature(f))
        song.append(get_time_signature_confidence(f))
        song.append(get_track_id(f))
        song.append(get_year(f))
        
    return song


def  readIn(df):
    tic = time.perf_counter()
    df_mapped = df.map(lambda x: read(x[1]))
    #df_mapped.count()
    df_mapped.take(1)
    toc = time.perf_counter()
    print(f"It took {toc - tic:0.4f} seconds")
    return df_mapped, toc-tic


# # Step 3: Convert rdd to dataframe
# convert the rdd (api version 1) to a dataframe (api version 2)
def convertToDF(df_in):
    tic = time.perf_counter()
    columns = ['artist_name', 'title', 'artist_familiarity', 
                'artist_hotttnesss', 'artist_id', 'artist_mbid', 
                'artist_playmeid', 'artist_7digitalid', 'artist_latitude', 
                'artist_longitude', 'artist_location', 'release', 
                'release_7digitalid', 
                'song_id', 'song_hotnesss', 'track_7digitalid', 
                'analysis_sample_rate', 
                'audio_md5', 'danceability', 'duration', 'end_of_fade_in', 
                'energy', 'key', 
                'key_confidence', 'loudness', 'mode', 'mode_confidence', 
                'start_of_fade_out', 
                'tempo', 'time_signature', 
                'time_signature_confidence', 'track_id', 'year']
    df = df_in.toDF(columns)
    toc = time.perf_counter()
    #df.printSchema()
    print(f"It took {toc - tic:0.4f} seconds")
    return df, toc-tic


def convertTypes(df):
    tic = time.perf_counter()
    # change types
    from pyspark.sql import types 
    ['BinaryType', 'BooleanType', 'ByteType', 'DateType', 
    'DecimalType', 'DoubleType', 'FloatType', 'IntegerType', 
    'LongType', 'ShortType', 'StringType', 'TimestampType']
    changedTypedf = df.withColumn("year", df["year"].cast("Integer"))\
                        .withColumn("track_id", df["track_id"].cast("Integer"))\
                        .withColumn("artist_id", df["artist_id"].cast("Integer"))\
                        .withColumn("song_id", df["song_id"].cast("Integer"))\
                        .withColumn("duration", df["duration"].cast("Float"))\
                        .withColumn("danceability", df["danceability"].cast("Float"))\
                        .withColumn("end_of_fade_in", df["end_of_fade_in"].cast("Float"))\
                        .withColumn("energy", df["energy"].cast("Float"))\
                        .withColumn("key_confidence", df["key_confidence"].cast("Float"))\
                        .withColumn("mode_confidence", df["mode_confidence"].cast("Float"))\
                        .withColumn("release_7digitalid", df["release_7digitalid"].cast("Integer"))\
                        .withColumn("song_hotnesss", df["song_hotnesss"].cast("Float"))\
                        .withColumn("start_of_fade_out", df["start_of_fade_out"].cast("Float"))\
                        .withColumn("loudness", df["loudness"].cast("Float"))\
                        .withColumn("tempo", df["tempo"].cast("Float"))

    toc = time.perf_counter()
    print(f"It took {toc - tic:0.4f} seconds")
    #changedTypedf.printSchema()
    return changedTypedf, toc-tic

    #changedTypedf.take(1)


# count the number of different artists in the dataset and count the numbers of songs release every year
def countArtistName(df):
    tic = time.perf_counter()
    df.groupBy('artist_name').count()   
    toc = time.perf_counter()
    print(f"It took {toc - tic:0.4f} seconds")
    return toc-tic


def countYear(df):
    tic = time.perf_counter()
    df.groupBy("year").count()
    toc = time.perf_counter()
    print(f"It took {toc - tic:0.4f} seconds")
    return toc-tic


# Let's get a bit more advanced: Calculate the average loudness of the songs of an artists tat puplished a song after 2000
def avgLoudness(df):
    tic = time.perf_counter()
    from pyspark.sql.functions import col, avg
    from pyspark.sql import functions as F
    #from pyspark.sql.functions import *
    left = df.select("artist_name").distinct().filter(df['year'] > 2000)

    right = df.groupBy("artist_name")\
                        .agg(avg(col("loudness"))\
                        .alias("avg_loudness"))\
                        .orderBy("avg_loudness", ascending=False)

    avg_loudness = left.join(right, left.artist_name == right.artist_name)\
                        .select(right["artist_name"], "avg_loudness")\
                        .orderBy("avg_loudness", ascending=False)#\.collect()       
    print(avg_loudness.take(5))
    toc = time.perf_counter()
    print(f"It took {toc - tic:0.4f} seconds")
    return toc-tic


def strong_scaling(number_of_nodes, problem_size, saving_path):
    """
    how the solution time varies with the number of processors for a fixed total problem size
    """
    print(
        "\n\n----------------------- STRONG SCALING ------------------- \n \n"
    )
    problem_size = round(problem_size / 55 ) # in average, there are 55 files per folder (1000000 / 18278)
    print(f"new problem size: {problem_size}")
    with open(f'{saving_path}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=',',
                                quotechar='|', quoting=csv.QUOTE_MINIMAL)
        header= ["processors", "readin", "mapping", "converting", "changetypes", "GroupByArtist", "GroupByYear", "Analysis"]
        writer.writerow (header)
        number_of_processors = [1]
        for processors in number_of_processors:
            for i in range(3):
                conf = (SparkConf()
                    .setMaster("spark://192.168.2.110:7077")
                    .setAppName(f"Group14_strong_{processors}processors_take{i}")
                    .set("spark.executor.cores", processors) # if anything >2, does not run
                    .set("spark.pytspark.python","python3")
                    .set("spark.dynamicAllocation.enabled", False)
                    .set("spark.shuffle.service.enabled", False)
                    .set("spark.executor.memory", "2g")
                    .set("spark.local.dir", "/home/ubuntu/MillionSong/spark/tmp"))

                #sc = SparkContext(conf = conf)
                sc = SparkContext.getOrCreate(conf)
                spark = SparkSession(sc)
                times = []
                times.append(processors*number_of_nodes)
                print("load binary")
                folders_ = [f"/LDSA/dataset/{a}/{b}/{c}/" 
                            for a in string.ascii_uppercase 
                            for b in string.ascii_uppercase 
                            for c in string.ascii_uppercase]
                tic = time.perf_counter()
                folders = folders_[:problem_size]
                print(f"len of folders: {len(folders)}")
                
                loaded = sc.union([sc.binaryFiles("hdfs://192.168.2.110:9000" + f) for f in folders])
                toc = time.perf_counter()
                print(f"It took {toc - tic:0.4f} seconds")
                #print(f"loaded {getsizeof(loaded)} files")
                #loaded, time_loaded = load(problem_size)
                times.append(toc-tic)

                print("map to arrays")
                mapped, time_mapped = readIn(loaded)
                times.append(time_mapped)
                #print(f"loaded {mapped.count()} files")
                
                print("convert to DF")
                converted, time_convert = convertToDF(mapped)
                times.append(time_convert)

                print("convert types")
                convertedTypes, time_convertedTypes = convertTypes(converted)
                times.append(time_convert)

                print("count artists")
                time_artist = countArtistName(convertedTypes)
                times.append(time_artist)

                print("count Years")
                time_year = countYear(convertedTypes)
                times.append(time_year)

                print("compute Average Loudness")
                time_loudness = avgLoudness(convertedTypes)
                times.append(time_loudness)
                writer.writerow(times)

                sc.stop()

def weak_scaling(number_of_nodes, problem_size_per_processor, saving_path):
    """
    how the solution time varies with the number of processors for a fixed problem size per processor
    """
    print(
        "\n\n----------------------- WEAK SCALING ------------------- \n \n"
    )
    problem_size_per_processor = round(problem_size_per_processor /  55 ) # in average, there are 55 files per folder (1000000 / 18278)
    print(f"new problem size: {problem_size_per_processor}")
    number_of_processors = [1,2]

    for processors in number_of_processors:

        with open(f'{saving_path}_{processors}Processors.csv', 'w', newline='') as csvfile:

            writer = csv.writer(csvfile, delimiter=',',
                                    quotechar='|', quoting=csv.QUOTE_MINIMAL)
            header= ["processors", "readin", "mapping", "converting", "changetypes", "GroupByArtist", "GroupByYear", "Analysis"]
            writer.writerow (header)
            for i in range(2):
                conf = (SparkConf()
                    .setMaster("spark://192.168.2.110:7077")
                    .setAppName(f"Group14_weak_{processors}processors_take{i}")
                    .set("spark.executor.cores", processors) # if anything >2, does not run
                    .set("spark.pytspark.python","python3")
                    .set("spark.dynamicAllocation.enabled", False)
                    .set("spark.shuffle.service.enabled", False)
                    .set("spark.executor.memory", "2g")
                    .set("spark.local.dir", "/home/ubuntu/MillionSong/spark/tmp"))

                #sc = SparkContext(conf = conf)
                sc = SparkContext.getOrCreate(conf)
                spark = SparkSession(sc)
                times = []
                times.append(processors*number_of_nodes)
                print("load binary")
                tic = time.perf_counter()
                folders_ = [f"/LDSA/dataset/{a}/{b}/{c}/" 
                            for a in string.ascii_uppercase 
                            for b in string.ascii_uppercase 
                            for c in string.ascii_uppercase]
                folders = folders_[:processors * number_of_nodes * problem_size_per_processor]
                print(f"len of folders: {len(folders)}")

                loaded = sc.union([sc.binaryFiles("hdfs://192.168.2.110:9000" + f) for f in folders])
                toc = time.perf_counter()
                print(f"It took {toc - tic:0.4f} seconds")
                print(f"loaded {getsizeof(loaded)} files")
                times.append(toc-tic)

                print("map to arrays")
                mapped, time_mapped = readIn(loaded)
                times.append(time_mapped)

                print("convert to DF")
                converted, time_convert = convertToDF(mapped)
                times.append(time_convert)

                print("convert types")
                convertedTypes, time_convertedTypes = convertTypes(converted)
                times.append(time_convert)

                print("count artists")
                time_artist = countArtistName(convertedTypes)
                times.append(time_artist)

                print("count Years")
                time_year = countYear(convertedTypes)
                times.append(time_year)

                print("compute Average Loudness")
                time_loudness = avgLoudness(convertedTypes)
                times.append(time_loudness)
                writer.writerow(times)

                sc.stop()

weak_scaling(1,10000, "weakScaling_1nodes_10000perNode")
strong_scaling(1, 50000, "strongScaling_1nodes_50000inTotal")

