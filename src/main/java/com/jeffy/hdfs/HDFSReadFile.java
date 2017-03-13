/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hdfs;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;

/**
 * HDFS文件读取简单示例
 * 本示例在Hadoop 2.7中验证通过
 * 
 * @author Jeffy<renwu58@gmail.com>
 */
public class HDFSReadFile {
	
	/**
	 * 正常情况下，如果不指定URL的Handler Factory将会无法识别hdfs的协议，会出现如下错误：
	 * java.net.MalformedURLException: unknown protocol: hdfs
	 */
	static{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String path = "hdfs://master:8020/tmp/user.csv";
		//HDFSReadFile.readDataUseURL(path);
		try {
			HDFSReadFile.readDataUseFileSystem(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 直接利用java.net.URL来访问HDFS中的文件，并打印在控制台中
	 * 
	 * @param path 文件路径，例如： hdfs://master:8020/tmp/user.csv
	 */
	public static void readDataUseURL(String path){
		try(InputStream in = new URL(path).openStream()){
			IOUtils.copy(in, System.out);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	/**
	 * 例如用Hadoop的FileSystem API来实现文件的读取
	 * 
	 * @param path
	 * @throws IOException 
	 */
	public static void readDataUseFileSystem(String path) throws IOException{
		
		Configuration config = new Configuration();
		/**
		 * 获取FileSystem对象，可以根据不同场景使用不同的参数来构造
		 *  public static FileSystem get(Configuration conf) throws IOException
		 *	public static FileSystem get(URI uri, Configuration conf) throws IOException
		 *	public static FileSystem get(URI uri, Configuration conf, String user)
		 *	throws IOException
		 */
		FileSystem fs = FileSystem.get(URI.create(path), config);
		//获取输入流，此处其实是FSDataInputStream,继承的DataInputStream,并提供了很多随机访问文件位置的方法。
		//open方法默认使用4KB的缓存空间
		try(InputStream in = fs.open(new Path(path))){
			IOUtils.copy(in, System.out);
		}
	} 
	
}
