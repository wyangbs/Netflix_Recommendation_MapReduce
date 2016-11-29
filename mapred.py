from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
import numpy
from scipy import spatial
import math

class MRMovieSimilarity(MRJob):
    __count =0

    def configure_options(self):
        super(MRMovieSimilarity, self).configure_options()
        self.add_file_option('--files', dest = "file_loc", help='Path to movies.dat')
        
        self.add_passthrough_option(
            '-m', dest='movie_title',
            type='str', default=None, action = "append",
            help=('Specifying the movie title'))
            
        self.add_passthrough_option(
            '-k', dest='num_item',
            type='int', default=25,
            help=('Specifying the number of matches'))
            
        self.add_passthrough_option(
            '-p', dest='min_shared_noofrate',
            type='int', default=3,
            help=('Specifying minimum sharing number'))
        
        self.add_passthrough_option(
            '-l', dest='min_shared_score',
            type='int', default=0.4,
            help=('Specifying minimum shared rating'))

        
    def steps(self):
        return [
            MRStep( mapper=self.mapper_cleaning,
                   reducer=self.reducer_users_movies),
            MRStep( mapper=self.mapper_form_pairs,
                   reducer=self.reducer_combine_pairs),
            MRStep( mapper=self.mapper_form_vector,
                   mapper_init = self.combine_files,
                   reducer=self.reducer_similar_scores),
            MRStep( mapper=self.reducer_top_results)]
        
    def combine_files(self):
        self.names = {}
        
        with open(self.options.file_loc,'r') as movie:
            for line in movie:
                arr = line.split("::")
                if len(arr) > 1:
                    self.names[int(arr[0])] = arr[1].decode('utf-8','ignore')
                    #movieList.update({arr[0]:arr[1]})
        
    def mapper_cleaning(self, key ,line):
        (userid, movieid, rating, time) = line.split('::')
        #print(userid, (movieid,float(rating)))
        yield userid, (movieid,float(rating))


    def reducer_users_movies(self, userid, mappair):
        ratings = list()
        for movieid,rating in mappair:
            ratings.append((movieid, rating))
        #print(ratings)
        yield userid, ratings   
        
        
    def mapper_form_pairs(self,uid,ratings):
        #print(ratings)
        for val1, val2 in combinations(ratings,2):
            #print(val1,val2)
            m1 = val1[0]
            r1 = val1[1]
            m2 = val2[0]
            r2 = val2[1]
            #print (m1,m2),(r1,r2)
            yield (m1,m2),(r1,r2)
            
    def reducer_combine_pairs(self, mpair, rpair):
        ratingsList = list()
        for rate in rpair:
            ratingsList.append(rate)
        yield mpair, ratingsList
        #print (mpair, ratingsList)        
        
    def similarity(self,m1List, m2List):
        cor = 0
        cos_cor = 0
        if (len(m1List) and len(m2List) != 1) and (len(m1List) == len(m2List)):    
            cor = numpy.corrcoef(m1List, m2List)[0][1]
            if math.isnan(cor):
                cor = 0
        if len(m1List) == len(m2List):
            cos_cor = 1 - spatial.distance.cosine(m2List, m2List)
        avg_cor = 0.5* (cor + cos_cor)
        return avg_cor, cos_cor, cor, len(m1List)
    
    def mapper_form_vector(self,mpair,ratingsList):
        #print(ratings)
        vector1 = list()
        vector2 = list()
        for rate in ratingsList:
            vector1.append(rate[0])
            vector2.append(rate[1])
        #print(mpair[0], vector1)
        #print(mpair[1], vector2)
        score, cos, cor, noofpair = self.similarity(vector1, vector2)      
        movie1 =str(self.names[int(mpair[0])])
        movie2 = str(self.names[int(mpair[1])])
        yield (movie1,movie2),(score,cos, cor, noofpair)

    def reducer_similar_scores(self, mpair, scoreList):
        #matchList = list()   
        for m2, cos, cor, n in scoreList:     
            score =m2
            cosine = cos
            correlation = cor 
            noofpair = n
        #print(self.options.movie_title)
        for movie in self.options.movie_title:
            if mpair[0] == movie:
                #print(movie)
                if score >= self.options.min_shared_score:
                    if noofpair >= self.options.min_shared_noofrate:
                        #print(mpair[1], score)
                        #print((mpair[1]),(mpair[0],score,cosine,correlation, noofpair) )
                        yield((mpair[0]),(score,mpair[1],cosine,correlation, noofpair) )
            elif mpair[1] == movie:
                if score >= self.options.min_shared_score:
                    if noofpair >= self.options.min_shared_noofrate:
                        #print(mpair[0], score)
#                        print("before")                        
#                        current_array = [score,cosine,correlation, noofpair]
#                        print(current_array)                        
#                        desired_array = [int(numeric_string) for numeric_string in current_array]
#                        print((mpair[1],mpair[0]),desired_array)                       
                        #print((mpair[1]),(mpair[0],score,cosine,correlation, noofpair) )
                        yield((mpair[1]),(score,mpair[0],cosine,correlation, noofpair) )
                        

    def reducer_top_results(self, movie, score):
        noofsearch = self.options.num_item
        #print(self.__count)
#        for m,s,cos,cor,n in score:
#            movie_sim = m
#            scores = s
#            cosine = cos
#            correlation = cor 
#            noofpair = n  
        if self.__count < noofsearch:
            self.__count = self.__count + 1            
            yield movie, (score)

if __name__ == '__main__':
    MRMovieSimilarity.run()