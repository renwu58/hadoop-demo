/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hdfs.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;

/**
 * 使用Hadoop提供的工厂方法解压常用压缩包
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class FileDecompressor {
	/**
	 * @param args
	 *            文件列表
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		//初始化配置项
		Configuration conf = new Configuration();
		// 创建一个解码器工厂类
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		for (String uri : args) {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path inputPath = new Path(uri);
			// 通过文件名称自动识别对应的解码器，解码器可以在参数：io.compression.codecs中定义
			CompressionCodec codec = factory.getCodec(inputPath);
			// 如果找不到对应的文件的解码器，则继续下一个文件
			if (codec == null) {
				System.err.println("No codec found for " + uri);
				continue;
			}
			String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
			try (InputStream in = codec.createInputStream(fs.open(inputPath));
					OutputStream out = fs.create(new Path(outputUri))) {
				IOUtils.copyBytes(in, out, conf);
			}
		}
	}

}
