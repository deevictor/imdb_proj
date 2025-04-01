to start the app:

launch main.py
to fill in the data for the dimension tables principals, names and aliases, 3 streams has been created.
copy name.basics.tsv to data/names_stream to launch names stream
copy principals.basics.tsv to data/principals_stream to launch principals stream
copy alias.basics.tsv to data/aliases_stream to launch aliases stream

after that you can copy data/title.ratings.tsv to data/title_ratings_stream to launch stream_ratings

1. the top 10 movies with a minimum of 500 votes with the ranking determined by:
(numVotes/averageNumberOfVotes) * averageRating will be displayed in the console.

2. For these 10 movies, the persons who are most often credited and list the
different titles of the 10 movies will be also displayed in the console.