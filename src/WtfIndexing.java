import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by Arezou on 2017-02-11.
 * This WtfIndexing class includes two classes, WtfIndexingMapper
 * class and WtfIndexingReducer class.
 * NOTE: This MapReducer job works even user id is defined by number or string.
 * The responsibility of this class is that for each user Fi followed
 * by user X, the mapper emits Fi as the key and X as the value.
 * The reducer is the identity. It produces inverted lists of followers:
 * X, [ Y1, Y2, ... , Yk ]
 * where the Yi all follow user X.
 */ 
public class WtfIndexing {
    /**
     * The WtfIndexingMapper class emits Fi as the key and X as the value
     * for each user Fi followed by user X.
     */
    public static class WtfIndexingMapper extends Mapper<Object, Text, Text, Text>{
        Text user = new Text();
        /**
         * @param key is the key of the chunk of data.
         * @param value is the value of the chunk or the information
         *              that we have in a chunk. In this example, value
         *              is a line of document. The value is: X  F1 F2 ...  Fn
         *              X is a user and follows F1 to Fn.
         *              The map method emits (Fi, X) and (X, -Fi).
         * @param context hadoop mapper context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            // First item of the iteration is a user(X).
            user.set(itr.nextToken());
            // In this part, we iterate over all F1 to Fn and emit (Fi, X) and (X, -Fi).
            while (itr.hasMoreTokens()){
                Text followed_by_user = new Text();
                followed_by_user.set(itr.nextToken());
                context.write(followed_by_user, user);
                followed_by_user.set("-" + followed_by_user.toString());
                context.write(user, followed_by_user);
            }
        }
    }

    /**
     * The WtfIndexingReducer class produces inverted lists of followers:
     * X, [ Y1, Y2, ... , Yk ]
     * where the Yi all follow user X.
     */
    public static class WtfIndexingReducer extends Reducer<Text, Text, Text, Text>{
        /**
         * The reducer method receives a list of [ Y1, Y2, ... , Yk ] for X (user).
         * The reducer method emits an inverted list of followers in this string format:
         * "X    Y1 Y2 ...Yk" and hadoop writs this list in the output file.
         * @param key is a userId.
         * @param values are list of users who followed user (key).
         * @param context hadoop reducer context.
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer str = new StringBuffer();
            //It iterates over values (Y1, Y2, ... , Yk) and appends to a string.
            for(Text value: values){
                str.append(" " + value.toString());
            }
            Text result = new Text();
            result.set(str.toString());
            //It emit (x, result) where result is "Y1 Y2 ...Yk".
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Common.jobRunner(conf, "wtf indexing", WtfIndexing.class, WtfIndexingMapper.class,
                WtfIndexingReducer.class, null, Text.class, Text.class, Text.class, Text.class,
                new Path(args[0]), new Path(args[1]));
    }
}
