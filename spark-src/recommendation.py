class Recommendation:
    """
    Created by Arezou on 2017-02-12.
    This Class describe a recommendation, and a recommendation has a
    user_Id and a number of followed user by this user in common (common_user_count).
    """
    def __init__(self, user_Id, common_user_count):
        """
        :param user_Id: user id as a str
        :param common_user_count: is the number of followed users by this
                                  user in common.
        """
        self.user_Id = user_Id
        self.common_user_count = common_user_count

    def __str__(self):
        """
        :return: string of Recommendation Object in this style: user_Id(common_user_count)
        """
        return str(self.user_Id) + '(' + str(self.common_user_count) + ')'

    def __gt__(self, other):
        """
        :param other: comparing two recommendation objects based on the common_user_count.
        :return: True if first recommendation object has more common_user_count than second one.
                 Otherwise, returns False.
        """
        return self.common_user_count > other.common_user_count