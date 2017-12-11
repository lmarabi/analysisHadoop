package org.umn.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.umn.hadoop.SortJob.MbrMapper;
import org.umn.hadoop.SortJob.MbrReducer;
import org.umn.hadoop.dayDriver.keywordDayDriver;
import org.umn.index.SortTimeJob;
import org.umn.index.SortTimeJob.SortTimeMapper;
import org.umn.index.SortTimeJob.SortTimeReducer;
import org.umn.index.TimeDriver;

public class Main {

	public static void main(String[] args) throws Exception {
		int result = 0;

		Path input = args.length > 0 ? new Path(args[0]) : new Path(
				"/export/scratch/louai/scratch1/workspace/dataset/analysisHadoop/data/2014-12-31/");
		Path output1 = args.length > 1 ? new Path(args[1]) : new Path(
				"/export/scratch/louai/scratch1/workspace/dataset/analysisHadoop/data/hdfs/KeywordDayData");
		Path output2 = args.length > 2 ? new Path(args[2]) : new Path(
				"/export/scratch/louai/scratch1/workspace/dataset/analysisHadoop/data/hdfs/sortedkeyword");
		String mbrFile = args.length > 2 ? args[3] : "/export/scratch/louai/scratch1/workspace/dataset/analysisHadoop/data/quadtree_mbrs.txt";
		String operation = args.length > 2 ? args[4] : "keywordDay";
		args = new String[4];
		args[0] = input.toString();
		args[1] = output1.toString();
		args[2] = output2.toString();
		args[3] = mbrFile.toString();

		if (operation.equals("keyword")) {

			result = ToolRunner.run(new Configuration(), new KeywordDriver(),
					args);

			System.out.println("Job1 dist finish with value: " + result);

			if (result == 0) {

				// Second Map-Reduce Job
				JobConf conf2 = new JobConf();
				FileSystem outfs2 = output2.getFileSystem(conf2);
				outfs2.delete(output2, true);
				Job job2 = Job.getInstance(conf2, "Keywords Sort");
				// set the class name
				job2.setJarByClass(SortJob.class);
				job2.setMapperClass(MbrMapper.class);
				job2.setReducerClass(MbrReducer.class);
				// set the output data type class
				job2.setMapOutputKeyClass(Text.class);
				job2.setMapOutputValueClass(KeyValueItems.class);
				// reducer data type
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);

				FileInputFormat.setInputDirRecursive(job2, true);
				// Set the hdfs
				FileInputFormat.addInputPath(job2, output1);
				FileOutputFormat.setOutputPath(job2, output2);
				job2.setNumReduceTasks(1);
				result = job2.waitForCompletion(true) ? 0 : 1;
				System.out.println("Job2 dist finish with value: " + result);

			}
		} else if (operation.equals("time")) {

			result = ToolRunner
					.run(new Configuration(), new TimeDriver(), args);

			System.out.println("Job1 dist finish with value: " + result);

			if (result == 0) {

				// Second Map-Reduce Job
				JobConf conf2 = new JobConf();
				FileSystem outfs2 = output2.getFileSystem(conf2);
				outfs2.delete(output2, true);
				Job job2 = Job.getInstance(conf2, "time sort");
				// set the class name
				job2.setJarByClass(SortTimeJob.class);
				job2.setMapperClass(SortTimeMapper.class);
				job2.setReducerClass(SortTimeReducer.class);
				// set the output data type class
				job2.setMapOutputKeyClass(Text.class);
				job2.setMapOutputValueClass(KeyValueItems.class);
				// reducer data type
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);

				FileInputFormat.setInputDirRecursive(job2, true);
				// Set the hdfs
				FileInputFormat.addInputPath(job2, output1);
				FileOutputFormat.setOutputPath(job2, output2);
				job2.setNumReduceTasks(1);
				result = job2.waitForCompletion(true) ? 0 : 1;
				System.out.println("Job2 dist finish with value: " + result);

			}
		}else if(operation.equals("keywordDay")){
			result = ToolRunner.run(new Configuration(), new keywordDayDriver(),
					args);

			System.out.println("Job1 dist finish with value: " + result);
		
		}else {
		
			System.out
					.println("This hadoop program have two operation used for fast analyzing quadtree for both:"
							+ "\n1) keyword"
							+ "\n2) time"
							+ "\n3) keywordDay"
							+ "\nPass the paramerter as the follwoing:"
							+ "\n$hadoop jar ...jar /input /output1 /output2 /quadtree_mbr.txt operation"
							+ "\n$hadoop jar ...jar /input /output whatever whatever keywordDay");
		}
	}

}
