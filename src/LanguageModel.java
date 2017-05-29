import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class LanguageModel {

    public static class LanguageModelMapper extends Mapper<LongWritable, Text, Text, Text> {

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

            for (int i = 0; i + maxWords < tokens.length; ++i) {
                StringBuilder pref = new StringBuilder();
                pref.append(tokens[0]);
                for (int j = i + 1; j < i + maxWords; ++j) {
                    context.write(new Text(pref.toString()), new Text(tokens[j]));
                    pref.append(" ");
                    pref.append(tokens[j]);
                }
            }
        }
    }

    public static class LanguageModelReducer extends Reducer<Text, Text, LangDBOutputWritable, NullWritable> {

        private int numTopHit;

        private class AutoCompleteEntry {
            private String followingWord;
            private int count;

            private AutoCompleteEntry(String word, int c) {
                followingWord = word;
                count = c;
            }
        }

        private class AutoCompleteEntryComparator implements Comparator<AutoCompleteEntry> {
            @Override
            public int compare(AutoCompleteEntry a, AutoCompleteEntry b) {
                return a.count - b.count;
            }
        }

        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            numTopHit = config.getInt(Constants.NUM_TOP_HIT, Constants.DEFAULT_NUM_TOP_HIT);
        }

        @Override
        public void reduce(Text key, Iterable<Text> vals, Context context)
                throws IOException, InterruptedException {

            HashMap<String, Integer> phraseCount = new HashMap<>();
            for (Text text : vals) {
                String k = text.toString();
                if (phraseCount.containsKey(k)) {
                    phraseCount.put(k, phraseCount.get(k) + 1);
                } else {
                    phraseCount.put(k, 1);
                }
            }

            PriorityQueue<AutoCompleteEntry> minHeap =
                    new PriorityQueue<>(numTopHit + 1, new AutoCompleteEntryComparator());
            for (String k : phraseCount.keySet()) {
                minHeap.add(new AutoCompleteEntry(k, phraseCount.get(k)));
                if (minHeap.size() >= numTopHit) {
                    minHeap.poll();
                }
            }

            while (!minHeap.isEmpty()) {
                AutoCompleteEntry entry = minHeap.poll();
                context.write(new LangDBOutputWritable(key.toString(), entry.followingWord, entry.count), NullWritable.get());
            }
        }
    }
}
