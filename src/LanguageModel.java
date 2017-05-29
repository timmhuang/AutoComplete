import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class LanguageModel {

    public static class LanguageModelMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private int maxWords;

        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            maxWords = config.getInt(Constants.MAX_WORDS, Constants.DEFAULT_NGRAM);
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] tokens = line.trim().split("\\s+");
            IntWritable occurances = new IntWritable(Integer.parseInt(tokens[tokens.length - 1]));

            for (int i = 0; i + maxWords < tokens.length; ++i) {
                StringBuilder pref = new StringBuilder();
                pref.append(tokens[0]);
                for (int j = i + 1; j < i + maxWords; ++j) {
                    String res = pref.toString() + ">" + tokens[j];
                    context.write(new Text(res), occurances);
                    pref.append(" ");
                    pref.append(tokens[j]);
                }
            }
        }
    }

    public static class LanguageModelReducer extends Reducer<Text, IntWritable, LangDBOutputWritable, NullWritable> {

        @Override
        public void reduce(Text key, Iterable<IntWritable> vals, Context context)
                throws IOException, InterruptedException {
            String[] tokens = key.toString().split("[>]");

            String startPhrase = tokens[0];
            String followingWord = tokens[1];
            int count = 0;
            for (IntWritable v : vals) {
                count += v.get();
            }
            context.write(new LangDBOutputWritable(startPhrase, followingWord, count), NullWritable.get());
        }
    }
}
