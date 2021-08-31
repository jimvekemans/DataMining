import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class PublisherBookTitles extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PublisherBookTitles(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), PublisherBookTitles.class);
        conf.setJobName("publisherbookcount");
        conf.setJarByClass(PublisherBookTitles.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setMapperClass(PublisherBookTitleMapper.class);
        conf.setCombinerClass(PublisherBookTitleReducer.class);
        conf.setReducerClass(PublisherBookTitleReducer.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }
}