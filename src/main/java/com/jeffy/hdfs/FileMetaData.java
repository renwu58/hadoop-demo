/*
 * Copyright AsiaInfo Authors.
 * 
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package com.jeffy.hdfs;

import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HDFS文件系统文件元数据信息获取
 * 
 * @author Jeffy<renwu58@gmail.com>
 *
 */
public class FileMetaData {
	public static void main(String[] args) {
		FileMetaData.showFileStatusForFile("hdfs://master:8020/tmp/user.csv");
	}
	public static void showFileStatusForFile(String path){
		Configuration config = new Configuration();
		try {
			FileSystem fs = FileSystem.get(URI.create(path), config);
			FileStatus stat = fs.getFileStatus(new Path(path));
			System.out.println("File URI: " + stat.getPath().toUri().getPath());
			System.out.println("Is directory: " + stat.isDirectory());
			System.out.println("File length: " + stat.getLen());
			System.out.println("Modification Time: " + new Date(stat.getModificationTime()));
			System.out.println("File replications: " + stat.getReplication());
			System.out.println("File Block Size: " + (stat.getBlockSize()>>>10>>>10) + " MB");
			System.out.println("File Owner: " + stat.getOwner());
			System.out.println("File Group: " + stat.getGroup());
			System.out.println("File Permission: " + stat.getPermission().toString());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
