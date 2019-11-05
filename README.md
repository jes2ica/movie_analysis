# Movie Analysis
 
 This repository includes the Scala project of data preprocessing. 
 
 ## DataSet
 Kaggle “Movies Dataset” (use these files only: movies_metadata.csv, credits.csv, ratings.csv).
 
 ## Implementation Details
 - Construct map from csv file using Spark.
 - Intepret json fields.
 - Build 4 csv files:
  - casts.csv (represents a cast, with label Talent)
  - crews.csv (represents a crew, with label Talent)
  - movies.csv (represent a movie, with labal Movie)
  - talent_movie_rel.csv (represent the relationship between talent and movie).
