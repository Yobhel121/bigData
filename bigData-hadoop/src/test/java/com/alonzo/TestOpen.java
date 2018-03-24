package com.alonzo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.alonzo.util.TestHdfsUtil;

public class TestOpen {
	public static void main(String[] args) throws IOException {
		test1();
	}

	static void test1() throws IOException {
		// hdfs dfs -cat 
		FileSystem fs = TestHdfsUtil.getFileSystem();
		InputStream is = fs.open(new Path("/alonzo/api/1.txt"));
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String line = null;
		while((line = br.readLine()) != null) {
			System.out.println(line);
		}
		br.close();
		is.close();
		fs.close();
	}
}
