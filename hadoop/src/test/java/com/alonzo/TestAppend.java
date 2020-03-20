package com.alonzo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestAppend {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.2.100:8020");
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path("/alonzo/api/createNewFile1.txt");
		FSDataOutputStream dos = fs.append(path);
		dos.write("Hello word".getBytes());
		dos.close();
		fs.close();
	}
}
