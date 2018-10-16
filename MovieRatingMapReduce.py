from mrjob.job import MRJob
from mrjob.step import MRStep

class OrderPopularMovies(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_movieid, reducer = self.reducer_count_movieids),
            MRStep(reducer= self.reduce_sort_popmovies)
        ]
    
    def mapper_get_movieids(self, _, line):
        (user_id, movie_id, rating, rating_time) = line.split('\t')
        yield movie_id, 1
    
    def reducer_count_movieids(self, key, values):
        yield str(sum(values)).zfill(5), key
        
    def reducer_sort_popmovies(self, count, movies):
        for movie in movies:
            yield movie, count
        
if __name__ == '__main__':
    OrderPopularMovies.run()
