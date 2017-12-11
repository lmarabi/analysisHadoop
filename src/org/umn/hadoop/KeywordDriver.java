package org.umn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KeywordDriver extends Configured implements Tool {
	
	@Override
	public int run(String[] args) throws Exception {
		int result = 0;
		Path input = args.length > 0 ? new Path(args[0]) : new Path(
				System.getProperty("user.dir") + "/data/*/*/");
		Path output1 = args.length > 1 ? new Path(args[1]) : new Path(
				System.getProperty("user.dir") + "/hdfs/hdfsoutput");
		Path output2 = args.length > 2 ? new Path(args[2]) : new Path(
				System.getProperty("user.dir") + "/hdfs/result");
		String mbrFile = args.length > 2 ? args[3] : System
				.getProperty("user.dir") + "/hdfs/quadtree_mbrs.txt";

		// First Map-Reduce Job
		JobConf conf = new JobConf(getConf(), KeywordDriver.class);
		//DistributedCache.addCacheFile(new Path(mbrFile).toUri(), conf);
		FileSystem outfs = output1.getFileSystem(conf);
		outfs.delete(output1, true);

		conf.setJobName("Keyword Driver");
		conf.set("mbrFile", mbrFile);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(KeywordJobMapper.class);
		conf.setCombinerClass(KeywordJobReducer.class);
		conf.setReducerClass(KeywordJobReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, input);
		FileOutputFormat.setOutputPath(conf, output1);

	    JobClient.runJob(conf).waitForCompletion();
		System.out.println("Job1 finish");
		return 0;


	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new KeywordDriver(), args);
		System.exit(res);

	}



}