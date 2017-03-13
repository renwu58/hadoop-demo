/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.mr;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private final IntWritable results = new IntWritable(0); 
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws  IOException, InterruptedException{
		final AtomicInteger sum = new AtomicInteger(0);
		values.forEach((iw)->{
			sum.set(sum.get() + iw.get());
		});
		results.set(sum.get());
		context.write(key, results);
	}
	
}
