import java.util.List;

/**
 * Created by Mojtaba on 2017-02-11.
 * This Class describe a recommendation, and a recommendation has a
 * user id and a number of followed user by this user in common.
 */
public class Recommendation implements Comparable {
    /**
     * user Id as a string
     */
    private String userId;
    /**
     * the number of followed user by this user in common as an int.
     */
    private int commonUserCount;
    /**
     * Constructor of Recommendation class
     * @param userId user id as a String
     * @param commonUserCount number of followed user by this user in common as an int number.
     */
    public Recommendation(String userId, int commonUserCount) {
        this.userId = userId;
        this.commonUserCount = commonUserCount;
    }
    /**
     * This method override compareTo method because it is used for sorting
     * a list or collection of Recommendations with sort method.
     * @param o other Recommendation as an Object
     * @return 0 if two Recommendations have same commonUserCount,
     * 1 if the commonUserCount of first Recommendation is
     * less than the commonUserCount of second Recommendation,
     * and -1 if it is grater than the commonUserCount of second Recommendation.
     */
    @Override
    public int compareTo(Object o) {
        if(this.commonUserCount == ((Recommendation)o).commonUserCount)
            return 0;
        else
            return this.commonUserCount > ((Recommendation) o).commonUserCount ? -1 : 1;
    }
    /**
     * This method override toString method in order to have this structure "userId(commonUserCount)"
     * for converting to string of a Recommendation.
     * @return string of Recommendation Object in this style: userId(commonUserCount)
     */
    @Override
    public String toString() {
        return this.userId + "(" + this.commonUserCount + ")";
    }
    /**
     * it received a list of recommendations and return convert it to a string format.
     * @param Recommendations list of Recommendation Object
     * @return recommendations in a StringBuffer type in this style:
     * "userId1(commonUserCount1) userId2(commonUserCount2) ... userIdn(commonUserCountn)"
     */
    public static StringBuffer toStringBuffer(List<Recommendation> Recommendations){
        StringBuffer str = new StringBuffer("");
        for(Recommendation recommendation: Recommendations){
            str.append(" " + recommendation.toString());
        }
        return str;
    }
}