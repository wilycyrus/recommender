from mrjob.job import MRJob
from mrjob.step import MRStep
from itertools import combinations
from math import sqrt

class Recommender(MRJob):
    def steps(self):
        return [
            MRStep(mapper = self.individual_ratings, reducer = self.ratings_by_user),
            MRStep(mapper = self.item_pairs, reducer = self.similarity),
            MRStep(mapper = self.order_by_similarity, reducer = self.output)
        ]
        
    def individual_ratings(self, _, line):
        user_id, item_id, rating, _ = line.split('\t')
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
        occurrence = 0
        for x, y in rating_pair:
            x = int(x)
            y = int(y)
            sum_xx += x*x
            sum_yy += y*y
            sum_xy += x*y
            sum_x += x
            sum_y += y
            occurrence += 1
        
        denom = sqrt(occurrence*sum_xx - sum_x*sum_x)*sqrt(occurrence*sum_yy - sum_y*sum_y)
        correlation = (occurrence*sum_xy - sum_x*sum_y)/denom if denom else 0.0
        correlation = (correlation + 1.0)/2.0
        
        yield item_pair, (correlation, occurrence)
        
    def order_by_similarity(self, item_pair, values):
        similarity, occurrence = values
        yield (item_pair[0], similarity), (item_pair[1], occurrence)
        
    def output(self, key, values):
        item1, similarity = key
        for item2, occurrence in values:
            yield None, (item1, item2, similarity, occurrence)

if __name__ == '__main__':
    Recommender.run()