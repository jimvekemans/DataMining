import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

public class PublisherBookCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PublisherBookCount(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), PublisherBookCount.class);
        conf.setJobName("publisherbookcount");
        conf.setJarByClass(PublisherBookCount.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

        private final static Text one = new Text("1");

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString();
            CSVParser csvParser = new CSVParserBuilder().withSeparator(';').build();
            CSVReader csvReader = new CSVReaderBuilder(new StringReader(line)).withCSVParser(csvParser).build();
            String[] bookParts = csvReader.readNext();
            Text publisher = new Text(bookParts[4]);
            output.collect(publisher, one);
        }
    }

    static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += Integer.parseInt(values.next().toString());
            }
            output.collect(key, new Text(String.valueOf(sum)));
        }
    }
}