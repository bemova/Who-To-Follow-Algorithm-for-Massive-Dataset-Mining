import os
import sys
from pyspark import SparkContext, SparkConf
from itertools import combinations

os.environ["SPARK_HOME"] = "/usr/local/spark"
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "ipython"


class Recommendation:
    """
    Created by Arezou on 2017-02-12.
    This Class describe a recommendation, and a recommendation has a
    user_Id and a number of followed user by this user in common (common_user_count).
    """
    def __init__(self, user_id, common_user_count):
        """
        :param user_id: user id as a str
        :param common_user_count: is the number of followed users by this
                                  user in common.
        """
        self.user_id = user_id
        self.common_user_count = common_user_count

    def __str__(self):
        """
        :return: string of Recommendation Object in this style: user_Id(common_user_count)
        """
        return str(self.user_id) + '(' + str(self.common_user_count) + ')'

    def __gt__(self, other):
        """
        :param other: comparing two recommendation objects based on the common_user_count.
        :return: True if first recommendation object has more common_user_count than second one.
                 Otherwise, returns False.
        """
        return self.common_user_count > other.common_user_count
pass


def indexing(pair):
    """
    The indexing method receives a pair of user X and followed users by X.
    for example it receives (X, Fi) as a pair which X follows Fi.
    This method returns (Fi, X) and (X, -Fi).
    :param pair: (X, Fi) as a pair which X follows Fi.
    :return: is a list of pairs [(Fi, X), (X, -Fi)].
    """
    user, followed_by_user = pair
    result = []
    for f in followed_by_user:
        result.append((f, user))
        result.append((user, '-' + f))
    return result


def similarity_mapper(pair):
    """
    this method receives a pair that the key is User X, and the users who follow user X.
    (X, [Y1, .....Yn, -F1, ..., -Fi]) where Fi is a list of direct relation to the user X.
    This method calculates all pairs (Yi, Yj) and (Yj, Yi) where i ∈ [1, k], j ∈ [1, k] and i != j, and all (X, -Fi)
    where Fi is a list of direct relation to the user X.
    :param pair: user X and followers of user X plus (X, [Y1, .....Yn, -F1, ..., -Fi])
    where Fi is a list of direct relation to the user X.
    :return: all pairs (Yi, Yj) and (Yj, Yi) where i ∈ [1, k], j ∈ [1, k] and i != j, and all (X, -Fi)
    where Fi is a list of direct relation to the user X.
    """
    user, followers = pair
    result = []
    # positive members are the items of followers where they don't follow user X directly.
    positive_members = [item for item in followers if not str(item).startswith("-")]
    # negative members are the items of followers where they follow user X directly.
    negative_members = [item for item in followers if str(item).startswith("-")]
    # for a list [F1 F2 .... Fn] combinations method gives all possible combination of two members.
    # for example [(F1, F2), (F1, F3),..., (F1, Fn), (F2, F3), ... , (F2, Fn), ...]
    common_followers = list(combinations(positive_members, 2))
    # add all calculated pairs to the result.
    result += common_followers
    # inverses will contain the inverses tuple of common_followers which are stored in the result.
    # Therefore, inverses will be [(F2, F1), (F3, F1),..., (Fn, F1), (F3, F2), ... , (Fn, F2), ...]
    inverses = []
    for element in result:
        inverses.append(tuple(reversed(element)))
    # add inverses tuples to the result.
    result += inverses
    # add direct relation between user X and the other user which are followed directly by use X
    for item in negative_members:
        result.append((user, item))
    return result


def remove_mutual_connections(pair):
    """
    After grouping by the key for the result of similarity mapper method we
    should remove all recommended users who had direct relation with user X
    for example: this method receives (1, [3 -3 -4 -5 2 3 4 2 4 5]) which means user X follows the list of users Fi
    before recommending a set of recommendations we should remove all user ids who are followed directly by user X.
    result should be like this: (1, [ 2 2])).
    :param pair: a pair of User X ,and a list of users which are common with user X.
    :return:
    """
    user, common_followed_users = pair
    # iterate over all user ids and keep all user ids that are not started with "-".
    common_followed_users = [user for user in common_followed_users if "-" + user not in common_followed_users]
    # iterate over all user ids and remove all users who are followed directly with the User X.
    common_followed_users = [user for user in common_followed_users if not str(user).startswith("-")]
    # keep all remaining users' ids in a dictionary whit the key of Fi and the number of occurrences of Fi in the list.
    common_followed_users = dict((i, common_followed_users.count(i)) for i in common_followed_users)
    result = []
    # iterate over the dictionary and create a list of recommendation for user X.
    for key, value in common_followed_users.items():
        recommendation = Recommendation(key, value)
        result.append(recommendation)
    # sort recommendation list in descending order with the __gt__ method which is implemented in Recommendation class.
    result.sort(reverse=True)
    # return a pair of user X with his or her recommendations.
    return user, result


def main(argv):
    """
    this python script solves  the "who to follow" problem in spark with python language.
    input: wtf-input/
    output: wtf-spark-output/
    local -> file:/home/arezou/Desktop/bigdata-assignments/wtf-input/
    hadoop -> hdfs://namenode:port/[file address]
    use same pattern for output
    :param argv: first is input and second is output address
    """
    conf = SparkConf().setMaster('local').setAppName('who to follow')
    sc = SparkContext(conf=conf)
    file = sc.textFile(argv[1])
    data = file.map(lambda x: (x.split()[0], x.split()[1:])).flatMap(indexing).groupByKey()\
        .flatMap(similarity_mapper).groupByKey().map(remove_mutual_connections)\
        .map(lambda x: x[0] + " " + (" ".join(str(recommendation) for recommendation in x[1])))
    data.repartition(1).saveAsTextFile(argv[2])
    sc.stop()


if __name__ == "__main__":
    main(sys.argv)
