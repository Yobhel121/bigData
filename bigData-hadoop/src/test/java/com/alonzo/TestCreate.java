package com.alonzo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestCreate {
	public static void main(String[] args) throws IOException {
		//test1();
		test2();
	}

	static void test1() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.2.100:8020");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream dos = fs.create(new Path("/alonzo/api/1.txt"));
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
		bw.write("hadoop大数据");
		bw.newLine();
		bw.write("离线数据分析平台");
		bw.close();
		dos.close();
		fs.close();
	}

	static void test2() throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.2.100:8020");
		FileSystem fs = FileSystem.get(conf);
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/alonzo/api/2.txt"), (short)1)));
		bw.write("www.alonzo.com");
		bw.close();
		fs.close();
	}
}
