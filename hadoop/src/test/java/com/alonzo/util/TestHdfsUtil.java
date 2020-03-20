package com.alonzo.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class TestHdfsUtil {
	public static Configuration getCofiguration() {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.2.100:8020");
		System.setProperty("hadoop.home.dir", "E:\\大数据（全）\\大数据软件工具\\hadoop-2.5.0-cdh5.3.6\\hadoop-2.5.0-cdh5.3.6");
		return conf;
	}

	public static FileSystem getFileSystem() throws IOException {
		return getFileSystem(getCofiguration());
	}

	public static FileSystem getFileSystem(Configuration conf) throws IOException {
		return FileSystem.get(conf);
	}
}
