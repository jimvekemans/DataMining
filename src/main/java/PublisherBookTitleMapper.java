import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.io.StringReader;
import java.util.Locale;

public class PublisherBookTitleMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String line = value.toString();
        CSVParser csvParser = new CSVParserBuilder().withSeparator(';').build();
        CSVReader csvReader = new CSVReaderBuilder(new StringReader(line)).withCSVParser(csvParser).build();
        String[] bookParts = csvReader.readNext();
        Text publisher = new Text(bookParts[4].toLowerCase(Locale.ROOT));
        Text bookTitle = new Text(bookParts[0].toLowerCase(Locale.ROOT));
        output.collect(publisher, bookTitle);
    }

}
