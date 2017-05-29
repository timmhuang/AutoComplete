import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.db.DBConfiguration;
import org.apache.hadoop.mapred.lib.db.DBOutputFormat;
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
        nGramLibBuild.waitForCompletion(true);

        // language model building job
        Configuration langModelConfig = new Configuration();
        if (args.length >= 4) {
            langModelConfig.set(Constants.MAX_WORDS, args[3]);
        }
        if (args.length >= 5) {
            langModelConfig.set(Constants.NUM_TOP_HIT, args[4]);
        }

		DBConfiguration.configureDB(
				langModelConfig,
				Constants.DB_DRIVER,
				Constants.DB_URL,
				Constants.DB_USERNAME,
				Constants.DB_PASSWORD
		);

        Job langModelBuild = Job.getInstance(langModelConfig);
        langModelBuild.setJobName("LangModel");
        langModelBuild.setJarByClass(Driver.class);

		langModelBuild.addArchiveToClassPath(new Path(Constants.DB_CONNECTOR_JAR_LOCATION));

        langModelBuild.setMapperClass(LanguageModel.LanguageModelMapper.class);
        langModelBuild.setReducerClass(LanguageModel.LanguageModelReducer.class);

        langModelBuild.setMapOutputKeyClass(Text.class);
        langModelBuild.setMapOutputValueClass(Text.class);
        langModelBuild.setOutputKeyClass(Text.class);
        langModelBuild.setOutputValueClass(NullWritable.class);

        langModelBuild.setInputFormatClass(TextInputFormat.class);
        langModelBuild.setOutputFormatClass(DBOutputFormat.class);

        DBOutputFormat.setOutput(
			langModelBuild,
			LangDBOutputWritable.TABLE_NAME,
			LangDBOutputWritable.FIELD_STARTING_PHRASE,
			LangDBOutputWritable.FIELD_FOLLOWING_WORD,
			LangDBOutputWritable.FIELD_COUNT);

        TextInputFormat.setInputPaths(langModelBuild, new Path(args[1]));
        System.exit(langModelBuild.waitForCompletion(true)?0:1);
    }
}
