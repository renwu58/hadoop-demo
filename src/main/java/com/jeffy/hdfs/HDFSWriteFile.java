/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

/**
 * HDFS 文件写入API使用总结
 * 文件写入主要通过FSDataOutputStream来实现
 * 
 * @author Jeffy<renwu58@gmail.com>
 *	
 */
public class HDFSWriteFile {

	/**
	 * 通过传入两个文件路径，将第一个路径的本地数据拷贝到远程hdfs文件系统中
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length < 2) {
			System.err.println("Please input two parameter!");
			System.out.println("Parameter: localfile hdfsfile");
			System.exit(1);
		}

		String localPath = args[0];
		String hdfsPath = args[1];
		//初始化配置
		Configuration config = new Configuration();
		//获取本地文件输入流
		try(InputStream in = new BufferedInputStream(new FileInputStream(localPath))){
			FileSystem fs = FileSystem.get(URI.create(hdfsPath), config);
			try(FSDataOutputStream out = fs.create(new Path(hdfsPath), new Progressable() {
				@Override
				public void progress() {
					System.out.println(".");
				}
			})){
				//读取本地数据，写入到OutputStream中,直接利用Hadoop的工具类：org.apache.commons.io.IOUtils
				IOUtils.copy(in, out);
				System.out.println("File copy finished.");
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
