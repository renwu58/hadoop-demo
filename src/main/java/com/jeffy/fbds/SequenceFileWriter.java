/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.fbds;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

/**
 * 使用SequenceFile格式写入文件到HDFS
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class SequenceFileWriter {
	// 待写入的数据
	private static final String[] DATA = { "One, two, buckle my shoe", "Three, four, shut the door",
			"Five, six, pick up sticks", "Seven, eight, lay them straight", "Nine, ten, a big fat hen" };

	public static void main(String[] args) throws IOException {
		// 写入的位置
		String uri = args[0];
		Configuration conf = new Configuration();
		Path path = new Path(uri);
		IntWritable key = new IntWritable();
		Text value = new Text();
		try (SequenceFile.Writer writer = SequenceFile.createWriter(conf, Writer.file(path),
				Writer.keyClass(key.getClass()), Writer.valueClass(value.getClass()))) {
			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(DATA[i % DATA.length]);
				System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
				writer.append(key, value);
			}
		}
	}

}
