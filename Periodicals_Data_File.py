import pandas
import csv
import json

#Periodical Issues Pipeline
#convert to JSON
periodical_issues = pandas.read_csv("LMS-DataStuff/trove-periodicals-data-main/periodical-issues.csv")

# Stage 1 Discovery & Profiling
print(periodical_issues.info())
#Check Data Types
print(periodical_issues.dtypes)
#Determine Expected Number of Null Values
print(periodical_issues.isnull().sum())
#Determine Number of Duplicates
print(periodical_issues.duplicated().sum())

