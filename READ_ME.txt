Problem Statement:

we'll process a large corpus of movie ratings for the purpose of providing recommendations. When you're done, your program will help you decide what to watch on Netflix tonight. For each pair of movies in the data set, you will compute their statistical correlation and cosine similarity (see this blog for a discussion of these and other potential similarity metrics). Since this isn't a statistics class, the calculation of similarity metrics for Python and Java will be provided, but you need to provide them with the correct inputs.
For this section of the assignment, we have two input data sets: a small set (details) for testing on your local machine or on digitalOcean and a large set (details) for running on Amazon's cloud. 
The small set data can be obtained by:

wget https://github.com/sidooms/MovieTweetings/blob/master/latest/ratings.dat 
wget https://github.com/sidooms/MovieTweetings/blob/master/latest/movies.dat 

The small set data can be obtained from http://grouplens.org/datasets/movielens/ 

For both data sets, you will find two input files:
•	movies.dat contains identification and genre information for each movie. 
	Lines are of the form: 
	MovieID::Movie Title::Genre  
	0068646::The Godfather (1972)::Crime|Drama 
	0181875::Almost Famous (2000)::Drama|Music
•	ratings.dat contains a series of individual user movie ratings. 
	Lines are of the form: 
	UserID::MovieID::Rating::Timestamp  
	120::0068646::10::1365448727 
	374::0181875::9::1374863640

In addition to these two input files, your program should take a few additional arguments:
•	-m [movie title]: The title of a movie for which we'd like to see similar titles. You should be able to accept multiple movie titles with more than one -m argument.
•	-k [number of items]: For each of the movies specified using -m, this specifies how many of the top matches we'd like to see. In other words, running with "-m The Godfather (1972) -k 10" would be asking for "the top ten movie recommendations if you liked The Godfather." (Default 25)
•	-l [similarity lower bound]: When computing movie similarity metrics, we'll produce a floating-point value in the range [-1, 1]. This input says to ignore any movie parings whose similarity metric is below this value. (Default 0.4)
•	-p [minimum rating pairs]: When computing similarity metrics, ignore any pair of movies that don't have at least this many shared ratings. (Default 3)
