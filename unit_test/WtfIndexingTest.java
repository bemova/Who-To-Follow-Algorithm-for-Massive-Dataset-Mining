import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by arezou on 13/02/17.
 */

public class WtfIndexingTest {
    MapDriver<Object, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;
    MapReduceDriver<Object, Text, Text, Text, Text, Text> mapReduceDriver;
    @Before
    public void setUp() {
        WtfIndexing.WtfIndexingMapper mapper = new WtfIndexing.WtfIndexingMapper();
        WtfIndexing.WtfIndexingReducer reducer = new WtfIndexing.WtfIndexingReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text(
                "1  3 4 5"));
        mapDriver.withOutput(new Text("4"), new Text("1"));
        mapDriver.withOutput(new Text("5"), new Text("1"));
        mapDriver.withOutput(new Text("3"), new Text("1"));
        mapDriver.withOutput(new Text("1"), new Text("-3"));
        mapDriver.withOutput(new Text("1"), new Text("-4"));
        mapDriver.withOutput(new Text("1"), new Text("-5"));
        mapDriver.runTest(false);
    }
    @Test
    public void testReducer() throws IOException {
        List<Text> values = new ArrayList<>();
        values.add(new Text("-5"));
        values.add(new Text("-3"));
        values.add(new Text("2"));
        values.add(new Text("3"));
        values.add(new Text("4"));
        values.add(new Text("-4"));
        reduceDriver.withInput(new Text("1"), values);
        //1	 -5 -3 2 3 4 -4
        reduceDriver.withOutput(new Text("1"), new Text(" -5 -3 2 3 4 -4"));
        reduceDriver.runTest(false);
    }
    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "1  3 4 5"));
        List<Text> values = new ArrayList<>();
        values.add(new Text("-5"));
        values.add(new Text("-3"));
        values.add(new Text("2"));
        values.add(new Text("3"));
        values.add(new Text("4"));
        values.add(new Text("-4"));
        reduceDriver.withInput(new Text("1"), values);
        reduceDriver.withOutput(new Text("1"), new Text(" -5 -3 2 3 4 -4"));
        reduceDriver.runTest(false);
    }
}
