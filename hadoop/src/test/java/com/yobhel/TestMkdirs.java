package com.yobhel;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.yobhel.util.TestHdfsUtil;

public class TestMkdirs {
	public static void main(String[] args) throws Exception{
		test1();
	}

	static void test1() throws IOException {
		FileSystem fs = TestHdfsUtil.getFileSystem();
		boolean mkdirsed = fs.mkdirs(new Path("/alonzo/api/mkdirs"));
		System.out.println(mkdirsed);
		fs.close();
	}
}
