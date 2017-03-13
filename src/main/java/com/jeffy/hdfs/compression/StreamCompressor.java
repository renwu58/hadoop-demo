/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hdfs.compression;

import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * 通过Hadoop提供的文件压缩类实现数据的流式压缩
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class StreamCompressor {

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		//压缩与解压实现类
		String codecClassname = args[0];

		Class<?> codecClass = Class.forName(codecClassname);

		Configuration conf = new Configuration();

		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);

		CompressionOutputStream out = codec.createOutputStream(System.out);

		IOUtils.copy(System.in, out);

		out.finish();

	}
}
