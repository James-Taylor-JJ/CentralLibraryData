import pandas
import csv
import json

#Periodical Issues Pipeline
#convert to Dataframe
periodical_issues = pandas.read_csv("LMS-DataStuff/trove-periodicals-data-main/periodical-issues.csv")


# Stage 1 Discovery & Profiling
print(periodical_issues.info())
#Check Data Types
print(periodical_issues.dtypes)
#Determine Expected Number of Null Values
print(periodical_issues.isnull().sum())
#Determine Number of Duplicates
print(periodical_issues.duplicated().sum())

#Stage 2 
#Remove Duplicate Values
periodical_issues_deduped = periodical_issues.drop_duplicates(subset=['id','title_id','description','date','url','pages','text_download_url'])
print(list(periodical_issues_deduped.columns))
#Replace Blank Spots with "Null"
periodical_issues_no_nulls = periodical_issues_deduped.replace({float('NaN'): "N/A"})
#Convert Everything to String
periodical_issues_corrected = periodical_issues_no_nulls.map(str)
#Remove Unnecessary Columns
periodical_issues_tailored = periodical_issues_corrected.drop(columns=['url','title','text_download_url'])
periodical_issues_tailored = periodical_issues_tailored.rename(columns={'id':'issue_id' ,'title_id':'id','description':'issue_number', 'date':'date_published','pages':'page_count'})
periodical_issues_tailored = periodical_issues_tailored[['id','issue_id','issue_number','date_published','page_count']]
#Stage 3

first_col = periodical_issues_tailored.columns[0]       # column 1
cols_to_merge = periodical_issues_tailored.columns[1:5] # columns 2–5
# --- CREATE LIST OF DICTIONARIES ---
periodical_issues_tailored["insert"] = periodical_issues_tailored[cols_to_merge].apply(lambda row: [{col: row[col] for col in cols_to_merge if pandas.notna(row[col])}], axis=1)
# --- KEEP ONLY FIRST COLUMN + MERGED COLUMN ---
periodical_issues_insert = periodical_issues_tailored[[first_col, "insert"]]

#Stage 4

#Periodical Titles Pipeline
#Convert to Dataframe
periodical_titles = pandas.read_csv("LMS-DataStuff/trove-periodicals-data-main/periodical-titles.csv")
# Stage 1 Discovery & Profiling
print(periodical_titles.info())
    #Check Data Types
print(periodical_titles.dtypes)
    #Determine Expected Number of Null Values
print(periodical_titles.isnull().sum())
print(periodical_titles.duplicated().sum())

#Stage 2 
#Remove Duplicate Values
periodical_titles_deduped = periodical_titles.drop_duplicates(subset=['id','title','description','publisher','trove_url','download_text','issue_count','start_date','end_date','start_year','end_year','extent','place','issn','catalogue_url'])
print(list(periodical_issues_deduped.columns))
#Replace Blank Spots with "Null"
periodical_titles_no_nulls = periodical_titles_deduped.replace({float('NaN'): "N/A"})
#Convert Everything to String
periodical_titles_corrected = periodical_titles_no_nulls.map(str)
#Removed Irrelevant Columns
periodical_titles_tailored = periodical_titles_corrected.drop(columns=['description','download_text','place','issn','catalogue_url','extent'])



#Stage 3

periodicals_merged = pandas.merge(periodical_issues_insert, periodical_titles_tailored,  on="id", how="outer")
periodicals_merged = periodicals_merged.rename(columns={'insert':'issue data'})
periodicals_merged = periodicals_merged[['id', 'title', 'publisher', 'trove_url', 'issue_count', 'issue data', 'start_date', 'end_date','start_year','end_year']]

#Stage 4
periodical_issues_tailored.to_csv("LMS-DataStuff/issue_unmerged.csv", index=False)
periodical_titles_tailored.to_csv("LMS-DataStuff/titles_unmerged.csv", index=False)
periodical_issues_insert.to_csv("LMS-DataStuff/periodicals_issues_insert.csv", index=False)
periodicals_merged.to_csv("LMS-DataStuff/periodicals_merged.csv", index=False)
periodicals_merged.to_json("LMS-DataStuff/periodicals_merged.json", orient="records", indent=2)





#periodical_issues_tailored.info()
#periodical_titles_tailored.info()

