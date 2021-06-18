ratings = LOAD '/user/maria_dev/ml-100k/u.data' AS (userID:int, movieID:int, rating:int, ratingTime:int);

metadata = LOAD '/user/maria_dev/ml-100k/u.item' USING PigStorage('|')
	AS (movieID:int, movieTitle:chararray, releaseDate:chararray, videoRealese:chararray, imdblink:chararray);
   
nameLookup = FOREACH metadata GENERATE movieID, movieTitle;

groupedRatings = GROUP ratings BY movieID;

avgRatings = FOREACH groupedRatings GENERATE group as movieID, 
		AVG(ratings.rating) as avgRating, COUNT(ratings.rating) AS numRatings;

OneStarMovies = FILTER avgRatings BY avgRating < 2.0;

OneStarMovieNames = JOIN OneStarMovies BY movieID, nameLookup BY movieID;

final = FOREACH OneStarMovieNames GENERATE nameLookup::movieTitle AS movieName,
	OneStarMovies::avgRating AS avgRating, OneStarMovies::numRatings AS numRatings;
    
finalSorted = ORDER final BY numRatings DESC;

DUMP finalSorted;
