import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class NGramLibraryBuilder {

    public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        int numGram;

                @Override
        public void setup(Context context) {
            Configuration conf = context.getConfiguration();
            numGram = conf.getInt(Constants.NUM_GRAM, Constants.DEFAULT_NGRAM);
        }
        
        //map method
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String sentence = value.toString();
            
            sentence = sentence.trim().toLowerCase();
            sentence = sentence.replaceAll("[^a-z]+", " ");
            String[] words = sentence.split("\\s+");

            if (words.length < 2) {
                return;
            }

            for (int i = 0; i + numGram < words.length; ++i) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < numGram; ++j) {
                    sb.append(words[i + j]);
                    sb.append(" ");
                    context.write(new Text(sb.toString().trim()), new IntWritable(1));
                }
            }
        }
    }

    public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> vals, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable v : vals) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
