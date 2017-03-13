/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.mr;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Jeffy<renwu58@gmail.com>
 * 实现将输入的文本文件进行安装空格拆分为单词 
 *  
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	//定义每次找到一个单词之后的的计数值，此处只能设置为1
	private final static IntWritable counter = new IntWritable(1);
	//代表一个单词
	private Text word = new Text();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		//按照空格拆
		 StringTokenizer itr = new StringTokenizer(value.toString());
		 while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, counter);//将每个拆分出来的单词写入hdfs
		}
	}
	
}
