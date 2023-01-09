package com.yobhel;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TextCreateNewFile {
	public static void main(String[] args) throws IOException {
		test1();
		test2();
	}

	/**
	 * 指定绝对路径
	 * 
	 * @throws IOException
	 */
	private static void test1() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.2.100:8020");
		FileSystem fs = FileSystem.get(conf);
		boolean created = fs.createNewFile(new Path("/alonzo/api/createNewFile1.txt"));
		System.out.println(created ? "创建成功" : "创建失败");
		fs.close();
	}

	/**
	 * 使用相对路径
	 * 
	 * @throws IOException
	 */
	private static void test2() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.2.100:8020");
		FileSystem fs = FileSystem.get(conf);
		boolean created = fs.createNewFile(new Path("createNewFile2.txt"));
		System.out.println(created);
		fs.close();
	}
}
