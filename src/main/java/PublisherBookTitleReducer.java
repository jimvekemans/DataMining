import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class PublisherBookTitleReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Set<Text> bookTitles = new TreeSet<>();
        while (values.hasNext()) {
            bookTitles.add(values.next());
        }
        StringBuilder outputValue = new StringBuilder();
        for(Text text : bookTitles){
            outputValue.append(text.toString());
            outputValue.append("; ");
        }

        output.collect(key, new Text(outputValue.toString()));
    }
}
