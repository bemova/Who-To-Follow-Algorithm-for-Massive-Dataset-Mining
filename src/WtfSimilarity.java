import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by Mojtaba on 2017-02-11.
 * This WtfSimilarity class includes two classes, WtfSimilarityMapper
 * class and WtfSimilarityReducer class.
 * NOTE: This MapReducer job works even user id is defined by a number or string.
 * This class is responsible for each inverted list X, [ Y1, Y2, ..., Yk ]
 * calculate how may users are common from followed list of two users and
 * compute recommendation list for each user.
 * This class uses the output of the indexing MapReduce job as an input.
 */
public class WtfSimilarity {
    /**
     * input file for this Mapper contains some information about user X like: X   Y1 Y2 ...Yk
     * The mapper emits all pairs (Yi, Yj) and
     * (Yj, Yi) where i ∈ [1, k], j ∈ [1, k] and i != j.
     */
    public static class WtfSimilarityMapper extends Mapper<Object, Text, Text, Text>{
        /**
         * @param key is the key of the chunk of data.
         * @param value is the value of the chunk or the information
         *              that we have in a chunk. In this example, value
         *              is a line of document. The value is: X  Y1 Y2 ...  Yn it also contains all Yi that is follwed By
         *              X directly. The mapper emits all pairs (Yi, Yj) and (Yj, Yi) where i ∈ [1, k], j ∈ [1, k] and i != j.
         *              It also emits X as a key and all Yi that is Followed directly(value) by X. in other word emits
         *              (X, Yi) where Yi start with "-".
         * @param context hadoop mapper context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            Text user = new Text();
            //first element in the iteration is User X
            user.set(itr.nextToken());
            // followers are a list of all followers of user X
            List<String> followers = new ArrayList<>();
            while (itr.hasMoreTokens()) {
                String follower_id = itr.nextToken();
                // if followers' id start with "-" means that follows directly User X.
                if(follower_id.startsWith("-")){
                    Text follower = new Text(follower_id);
                    // emit (user, and directed follower of X)
                    context.write(user, follower);
                }else{
                    // add follower id in the list of follower.
                    followers.add(follower_id);
                }
            }
            //iterates over followers and emits all pairs (Yi, Yj) and (Yj, Yi)
            // where i ∈ [1, k], j ∈ [1, k] and i != j
            for (int i = 0; i < followers.size(); i++) {
                Text follower1 = new Text(followers.get(i));
                for(int j = i + 1; j < followers.size(); j++) {
                    Text follower2 = new Text(followers.get(j));
                    context.write(follower1, follower2);
                    context.write(follower2, follower1);
                }
            }
        }
    }

    /**
     * The Reducer class responsibility is emitting all users with recommendations for each user for following.
     */
    public static class WtfSimilarityReducer extends Reducer<Text, Text, Text, Text>{
        /**
         * It receives a list X, [ F1, F2, ... ] where Fi appears exactly x times if X and Fi follow x people
         * in common. It counts the occurrences of Fi whenever Fi is not followed by X and sorts the
         * resulting recommendations by number of common followed people.
         * @param key userId of user X in a Text type
         * @param values is a list of [ F1, F2, ... ] where Fi is a user ids and appears exactly x times
         *               if X and Fi follow x people in common.
         *               some of Fi starts with "-" which means user X follows directly Fi.
         *
         * @param context hadoop reducer context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text result  = new Text();
            // list of all user ids of [ F1, F2, ... ]
            List<String> users = new ArrayList<>();
            for(Text value: values) {
                users.add(value.toString());
            }

            Map<String, Integer> map = new HashMap<>();
            // iterates over user ids which are not started with "-".
            for(String value : users){
                if(!value.startsWith("-")) {
                    // if we met the user id before and it exists in the map, we should add the number of occurrences.
                    if(map.containsKey(value)){
                        map.put(value, map.get(value) + 1);
                    /* else if we met a user id for the first time and the map does not contain the key
                       we should add it to the map in case that there is not a same user id in the users list
                       where user id start with the "-".
                     */
                    }else if(!users.stream().anyMatch( f -> f.equals("-" + value))){
                        map.put(value, 1);
                    }
                }
            }
            // list of recommendations for a user X.
            List<Recommendation> recommendations = new ArrayList<>();
            // iterates over map and creates a Recommendation object for each key value pair in the map.
            // key is the user id and value is the commonUserCount.
            for(Map.Entry<String, Integer> entry: map.entrySet()){
                recommendations.add(new Recommendation(entry.getKey(), entry.getValue()));
            }
            // descending sort of recommendation list based on commonUserCount.
            Collections.sort(recommendations);
            result.set(Recommendation.toStringBuffer(recommendations).toString());
            // emit(X, recommendations) key is user X and recommendation is a list of
            // recommendation for user X for following.
            context.write(key, result);
        }
    }
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Common.jobRunner(conf, "wtf similarity", WtfSimilarity.class,
                WtfSimilarityMapper.class, WtfSimilarityReducer.class, null,
                Text.class, Text.class, Text.class, Text.class, new Path(args[0]), new Path(args[1]));
    }
}
