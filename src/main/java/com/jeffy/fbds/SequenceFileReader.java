/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.fbds;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 读取SequenceFile文件
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class SequenceFileReader {

	public static void main(String[] args) {
		String uri = args[0];
		Configuration conf = new Configuration();
		Path path = new Path(uri);
		try (SequenceFile.Reader reader = new SequenceFile.Reader(conf, Reader.file(path))) {
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
				position = reader.getPosition();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
