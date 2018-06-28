import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by mojtaba on 13/02/17.
 */
public class WtfSimilarityTest {
    MapDriver<Object, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;
    @Before
    public void setUp() {
        WtfSimilarity.WtfSimilarityMapper mapper = new WtfSimilarity.WtfSimilarityMapper();
        WtfSimilarity.WtfSimilarityReducer reducer = new WtfSimilarity.WtfSimilarityReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "1  -5 -3 2 3 4 -4"));
        mapDriver.withOutput(new Text("1"), new Text("-5"));
        mapDriver.withOutput(new Text("1"), new Text("-3"));
        mapDriver.withOutput(new Text("1"), new Text("-4"));
        mapDriver.withOutput(new Text("2"), new Text("3"));
        mapDriver.withOutput(new Text("3"), new Text("2"));
        mapDriver.withOutput(new Text("2"), new Text("4"));
        mapDriver.withOutput(new Text("4"), new Text("2"));
        mapDriver.withOutput(new Text("3"), new Text("4"));
        mapDriver.withOutput(new Text("4"), new Text("3"));
        mapDriver.runTest(false);
    }
    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("-3"));
        values.add(new Text("-4"));
        values.add(new Text("2"));
        values.add(new Text("2"));
        values.add(new Text("3"));
        values.add(new Text("4"));
        values.add(new Text("3"));
        values.add(new Text("-5"));
        values.add(new Text("4"));
        values.add(new Text("5"));
        reduceDriver.withInput(new Text("1"), values);
        reduceDriver.withOutput(new Text("1"), new Text(" 2(2)"));
        reduceDriver.runTest(false);
    }
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "1  -5 -3 2 3 4 -4"));
        List<Text> values = new ArrayList<>();
        values.add(new Text("-3"));
        values.add(new Text("-4"));
        values.add(new Text("2"));
        values.add(new Text("2"));
        values.add(new Text("3"));
        values.add(new Text("4"));
        values.add(new Text("3"));
        values.add(new Text("-5"));
        values.add(new Text("4"));
        values.add(new Text("5"));
        reduceDriver.withInput(new Text("1"), values);
        reduceDriver.withOutput(new Text("1"), new Text(" 2(2)"));
        reduceDriver.runTest(false);
    }
}