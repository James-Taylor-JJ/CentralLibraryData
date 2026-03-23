import pandas
import csv
import ast

#Movie MetaData Pipeline
#convert to Dataframe
movie_metadata = pandas.read_csv("LMS-DataStuff/movies-archive/movies_metadata.csv")
movie_ids = pandas.read_csv("LMS-DataStuff/movies-archive/links.csv")
movie_keywords = pandas.read_csv("LMS-DataStuff/movies-archive/keywords.csv")
movie_credits = pandas.read_csv("LMS-DataStuff/movies-archive/credits.csv")


# Prep for Preliminary Merges

movie_metadata_stripped = movie_metadata.drop(columns=['budget','homepage','original_language','original_title','popularity', 'popularity', 'poster_path', 'production_countries', 'revenue', 'status', 'tagline', 'video', 'vote_average', 'vote_count'])
movie_metadata_prepped = movie_metadata_stripped.rename(columns={'id':'tmdb_id','adult':'rating', 'belongs_to_collection':'collection'})
movie_metadata_prepped = movie_metadata_prepped.map(str)
#print(movie_metadata_prepped.columns)
movie_keywords_prepped = movie_keywords.rename(columns={'id':'tmdb_id'})
movie_keywords_prepped = movie_keywords_prepped.map(str)
#print(movie_keywords_prepped.columns)
movie_ids_prepped = movie_ids.rename(columns={'tmdbId':'tmdb_id'})
movie_ids_prepped = movie_ids_prepped.map(str)
movie_ids_prepped = movie_ids_prepped.drop(columns=['imdbId'])
#print(movie_ids_prepped.columns)
movie_credits_prepped = movie_credits.rename(columns={'id':'tmdb_id'})
movie_credits_prepped = movie_credits_prepped.map(str)
#print(movie_credits_prepped.columns)

# Preliminary Merges
movie_ids_insert = pandas.merge(movie_metadata_prepped, movie_ids_prepped, on="tmdb_id", how="outer")
movie_keywords_insert = pandas.merge(movie_ids_insert, movie_keywords_prepped, on="tmdb_id", how="outer")
movie_credits_insert = pandas.merge(movie_keywords_insert, movie_credits_prepped,   on="tmdb_id", how="outer")
#print(movie_credits_insert.info)
movie_credits_insert.to_csv("LMS-DataStuff/movies-archive/merge_preview.csv", index=False)
movies_data = movie_credits_insert

# Second Cleaning
#Convert Everything to String
movies_data_corrected = movies_data.map(str)
#print(movies_data.dtypes())
#Remove Duplicate Values
movies_data_deduped = movies_data.drop_duplicates(subset=['tmdb_id','keywords','rating','collection','genres','imdb_id','overview','production_companies','release_date','runtime','spoken_languages','title'])
#print(movies_data_deduped.duplicated().sum())
#Replace Blank Spots with "Null"
movies_data_no_nulls = movies_data_deduped.replace({float('NaN'): "N/A"})
#print(movies_data_no_nulls.isnull().sum())
#Reorder Columns
movies_data_tailored = movies_data_no_nulls[['movieId', 'tmdb_id', 'imdb_id', 'title' , 'collection', 'overview','runtime', 'cast', 'crew', 'rating', 'genres','keywords', 'production_companies','release_date', 'spoken_languages']]
#print(movies_data_tailored)
movie_data_tailored = movies_data_tailored.rename(columns={'movieId':'movie_id'})
movies_data = movies_data_tailored

sample = movies_data.sample(n=383)
print(sample)
sample.to_json("LMS-DataStuff/movies-archive/movies_data_sample_data.json", orient='records', indent=2)

#Final Export
#movies_data.to_csv("LMS-DataStuff/movies-archive/movies_data.csv", index=False)
#movies_data.to_json("LMS-DataStuff/movies-archive/movies_data.json", orient='records', index=2)

# Dataset Halved#
#half = len(movies_data) // 2

#movies_data_pt1 = movies_data.iloc[:half]
#movies_data_pt2 = movies_data.iloc[half:]

#movies_data_pt1.to_csv("LMS-DataStuff/movies-archive/movies_data_part1.csv", index=False)
#movies_data_pt2.to_csv("LMS-DataStuff/movies-archive/movies_data_part2.csv", index=False)

# Dataset Quartered
#hunk = pandas.read_csv("LMS-DataStuff/movies-archive/movies_data_section1.csv")
#chunk_size = 8334

#for i in range(3):
    #start = i * chunk_size
    #end = start + chunk_size
    #hunk.iloc[start:end].to_json(f"LMS-DataStuff/movies-archive/movies_data_sliver{i+1}.json", orient='records', index=2)
