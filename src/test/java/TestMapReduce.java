import exercise4.Ex4AverageWordLength;
import exercise4.Ex4InvertedIndex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

public class TestMapReduce {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        protected Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, new IntWritable(1));
            }
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    @Test
    public void mapperBreakesTheRecord() throws IOException {
        new MapDriver<LongWritable, Text, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("msg1"), new IntWritable(1)),
                        new Pair<>(new Text("msg2"), new IntWritable(1)),
                        new Pair<>(new Text("msg1"), new IntWritable(1))
                ))
                .runTest();
    }

    @Test
    public void testSumReducer() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new WordCountReducer())
                .withInput(new Text("msg1"), Arrays.asList(new IntWritable(1), new IntWritable(1)))
                .withOutput(new Text("msg1"), new IntWritable(2))
                .runTest();
    }

    @Test
    public void testWordCount() throws IOException {
        new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>()
                .withMapper(new WordCountMapper())
                .withReducer(new WordCountReducer())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("msg1"), new IntWritable(2)),
                        new Pair<>(new Text("msg2"), new IntWritable(1))
                ))
                .runTest();
    }

    @Test
    public void lengthCountMapperBreakesTheRecord() throws IOException {
        new MapDriver<Object, Text, IntWritable, IntWritable>()
                .withMapper(new exercise2.WordLengthCount.TokenizerMapper())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new IntWritable(4), new IntWritable(1)),
                        new Pair<>(new IntWritable(4), new IntWritable(1)),
                        new Pair<>(new IntWritable(4), new IntWritable(1))
                ))
                .runTest();
    }

    @Test
    public void testLengthCountReducer() throws IOException {
        new ReduceDriver<IntWritable, IntWritable, IntWritable, IntWritable>()
                .withReducer(new exercise2.WordLengthCount.IntSumReducer())
                .withInput(new IntWritable(4), Arrays.asList(new IntWritable(1), new IntWritable(1),  new IntWritable(1)))
                .withOutput(new IntWritable(4), new IntWritable(3))
                .runTest();
    }

    @Test
    public void testWordLengthCount() throws IOException {
        new MapReduceDriver<Object, Text, IntWritable, IntWritable, IntWritable, IntWritable>()
                .withMapper(new exercise2.WordLengthCount.TokenizerMapper())
                .withReducer(new exercise2.WordLengthCount.IntSumReducer())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1"))
                .withOutput(new Pair<>(new IntWritable(4), new IntWritable(3)))
                .runTest();
    }


    @Test
    public void averageWordLengthMapperBreakesTheRecord() throws IOException {
        new MapDriver<Object, Text, Text, IntWritable>()
                .withMapper(new Ex4AverageWordLength.Ex4Mapper())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1000"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("m"), new IntWritable(4)),
                        new Pair<>(new Text("m"), new IntWritable(4)),
                        new Pair<>(new Text("m"), new IntWritable(7))
                ))
                .runTest();
    }

    @Test
    public void testAverageWordLengthReducer() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, DoubleWritable>()
                .withReducer(new Ex4AverageWordLength.Ex4Reducer())
                .withInput(new Text("m"), Arrays.asList(new IntWritable(4), new IntWritable(4), new IntWritable(7)))
                .withOutput(new Text("m"), new DoubleWritable(5))
                .runTest();
    }

    @Test
    public void testAverageWordLengthCount() throws IOException {
        new MapReduceDriver<Object, Text, Text, IntWritable, Text, DoubleWritable>()
                .withMapper(new Ex4AverageWordLength.Ex4Mapper())
                .withReducer(new Ex4AverageWordLength.Ex4Reducer())
                .withInput(new LongWritable(0), new Text("msg1 msg2 msg1000 prova1 prova2"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("m"), new DoubleWritable(5)),
                                new Pair<>(new Text("p"), new DoubleWritable(6))
                ))
                .runTest();
    }

    @Test
    public void invertedIndexMapperBreakesTheRecord() throws IOException {
        new MapDriver<Object, Text, Text, LongWritable>()
                .withMapper(new Ex4InvertedIndex.Ex4Mapper())
                .withInput(new LongWritable(0), new Text("msg1 msg1 msg2"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("msg1"), new LongWritable(0)),
                        new Pair<>(new Text("msg1"), new LongWritable(1)),
                        new Pair<>(new Text("msg2"), new LongWritable(2))
                ))
                .runTest();
    }

    @Test
    public void testInvertedIndexReducer() throws IOException {
        new ReduceDriver<Text, LongWritable, Text, Text>()
                .withReducer(new Ex4InvertedIndex.Ex4Reducer())
                .withInput(new Text("msg1"), Arrays.asList(new LongWritable(0), new LongWritable(1)))
                .withOutput(new Text("msg1"), new Text("[0, 1]"))
                .runTest();
    }

    @Test
    public void testInvertedIndexCount() throws IOException {
        new MapReduceDriver<Object, Text, Text, LongWritable, Text, Text>()
                .withMapper(new Ex4InvertedIndex.Ex4Mapper())
                .withReducer(new Ex4InvertedIndex.Ex4Reducer())
                .withInput(new LongWritable(0), new Text("msg1 msg1 msg2"))
                .withAllOutput(Arrays.asList(
                        new Pair<>(new Text("msg1"), new Text("[0, 1]")),
                        new Pair<>(new Text("msg2"), new Text("[2]"))
                ))
                .runTest();
    }
}
