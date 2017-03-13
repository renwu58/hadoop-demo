/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hdfs.compression;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;

/**
 * 使用CodecPool实现重用Compressors来提升批量压缩大量文件的效率
 * CodecPool可以重用压缩相关的对象，类似于数据库连接池。
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class FileCompressor {

	/**
	 * @param args
	 * 参数一指定压缩文件的名称，必须带压缩格式的默认扩展名
	 * 后面参数为需要添加到压缩文件中的文件列表
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		//获取解码器工厂
		CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		// For example for the 'GzipCodec' codec class name the alias are 'gzip' and 'gzipcodec'.
		CompressionCodec codec = factory.getCodecByName(args[0]);
		if (codec == null) {//检查是否找到合适的解码器
			System.err.println("Comperssion codec not found for " + args[0]);
			System.exit(1);
		}
		String ext = codec.getDefaultExtension();
		Compressor compressor = null;
		try {
			//根据CodecPool获取到Compressor
			compressor = CodecPool.getCompressor(codec);
			for(int i=1; i<args.length; i++){
				String filename = args[i] + ext;
				System.out.println("Compression the file " + filename);
				try(FileSystem outFs = FileSystem.get(URI.create(filename), conf);
						FileSystem inFs = FileSystem.get(URI.create(args[i]), conf);
						InputStream in = inFs.open(new Path(args[i]))){//创建文件系统对象，准备写文件
					//使用Compressor创建对应的写入流
					CompressionOutputStream out = codec.createOutputStream(outFs.create(new Path(filename)), compressor);
					//为了简单起见，目前不支持目录压缩
					IOUtils.copy(in, out);
					out.finish();//此处只能使用finish()方法，不能使用flush()来保存数据
					compressor.reset(); //每使用完一次，需要重置状态，否则下次使用会报错，提示java.io.IOException: write beyond end of stream
				}
			}
		} finally {//每次回收Compressor对象提供给下次使用
			CodecPool.returnCompressor(compressor);
		}
	}
}
