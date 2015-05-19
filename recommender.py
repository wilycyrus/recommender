from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from math import sqrt

class Recommender(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.individual_ratings, reducer = self.ratings_by_user),
            MRStep(mapper = self.item_pairs, reducer = self.similarity)
        ]
        
    def individual_ratings(self, _, line):
        user_id, item_id, rating = line.split('|')
        yield user_id, (item_id, rating)

    def ratings_by_user(self, user_id, values):
        item_count = 0
        rating_sum = 0
        ratings = []
        for item_id, rating in values:
            # item_count += 1
            # rating_sum += float(rating)
            ratings.append((item_id, rating))
        yield user_id, ratings
        
    def item_pairs(self, user_id, ratings):
        for item1, item2 in combinations(ratings, 2):
            yield (item1[0], item2[0]), (item1[1], item2[1])

    def similarity(self, item_pair, rating_pair):
        sum_x = 0
        sum_y = 0
        sum_xx = 0
        sum_yy = 0
        sum_xy = 0
        num_pairs = 0
        for x, y in rating_pair:
            x = int(x)
            y = int(y)
            sum_xx += x*x
            sum_yy += y*y
            sum_xy += x*y
            sum_x += x
            sum_y += y
            num_pairs += 1
        
        denom = sqrt(num_pairs*sum_xx - sum_x*sum_x)*sqrt(num_pairs*sum_yy - sum_y*sum_y)
        correlation = (num_pairs*sum_xy - sum_x*sum_y)/denom if denom else 0.0
        
        yield item_pair, correlation
        
if __name__ == '__main__':
    Recommender.run()