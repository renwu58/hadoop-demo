/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.mr;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class WordCount {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		String input = "hdfs://master:8020/tmp/jeffy/input/wordcount.txt";
		String output = "hdfs://master:8020/tmp/jeffy/output";
		Configuration config = new Configuration();
		/**
		 * 解决Windows中运行提示no jobCtrol问题
		 * http://stackoverflow.com/questions/24075669/mapreduce-job-fail-when-submitted-from-windows-machine
		 */
		config.set("mapreduce.app-submission.cross-platform", "true");
		config.set("mapred.remote.os", "Linux");
		try {
			Job job = Job.getInstance(config);
			//Windows中无法直接指定类来提交作业
			job.setJarByClass(WordCount.class);
			//下面这个必须有，否则无法正常执行，会提示类找不到
			job.setJar("D:\\bigdata\\mapreduce-demo\\src\\main\\java\\WordCount.jar");
			job.setJobName("Wordcount job");
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			job.setInputFormatClass(TextInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			TextInputFormat.setInputPaths(job, new Path(input));
			TextOutputFormat.setOutputPath(job, new Path(output));
			// Submit the job, then poll for progress until the job is complete
			try {
				job.waitForCompletion(true);
			} catch (ClassNotFoundException | InterruptedException e) {
				e.printStackTrace();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
