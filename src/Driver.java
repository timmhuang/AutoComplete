/**
 * Created by tim on 5/27/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
        Configuration nGramBuildConfig = new Configuration();
		nGramBuildConfig.set("textinputformat.record.delimiter", ".");
		nGramBuildConfig.set(Constants.NUM_GRAM, args[2]);
		
		//First Job 
	    Job nGramLibBuild = Job.getInstance(nGramBuildConfig);
	    nGramLibBuild.setJobName("NGram");
	    nGramLibBuild.setJarByClass(Driver.class);

	    nGramLibBuild.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
	    nGramLibBuild.setReducerClass(NGramLibraryBuilder.NGramReducer.class);

		nGramLibBuild.setOutputKeyClass(Text.class);
		nGramLibBuild.setOutputValueClass(IntWritable.class);

		nGramLibBuild.setInputFormatClass(TextInputFormat.class);
		nGramLibBuild.setOutputFormatClass(TextOutputFormat.class);

		TextInputFormat.setInputPaths(nGramLibBuild, new Path(args[0]));
		TextOutputFormat.setOutputPath(nGramLibBuild, new Path(args[1]));
        System.exit(nGramLibBuild.waitForCompletion(true)?0:1);
    }
}
