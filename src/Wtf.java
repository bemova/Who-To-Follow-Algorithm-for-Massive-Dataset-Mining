import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by Mojtaba on 12/02/17.
 * this class chaining two indexer and similarity MapReduce jobs.
 */
public class Wtf {
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Configuration conf = new Configuration();
        // indexing job starts first and hadoop waits for completion of this job.
        Common.jobRunner(conf, "wtf indexing", WtfIndexing.class, WtfIndexing.WtfIndexingMapper.class,
                WtfIndexing.WtfIndexingReducer.class, null, Text.class, Text.class, Text.class, Text.class,
                new Path(args[0]), new Path(args[1]));
        // after finishing first job second job will be run which is the similarity job.
        Common.jobRunner(conf, "wtf similarity", WtfSimilarity.class,
                WtfSimilarity.WtfSimilarityMapper.class, WtfSimilarity.WtfSimilarityReducer.class, null,
                Text.class, Text.class, Text.class, Text.class, new Path(args[1]+ "part-r-00000"), new Path(args[2]));
    }
}
