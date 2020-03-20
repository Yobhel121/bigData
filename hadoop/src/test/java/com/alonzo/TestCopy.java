package com.alonzo;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.alonzo.util.TestHdfsUtil;

public class TestCopy {
	public static void main(String[] args) throws Exception{
		//testCopyFromLocal();
		testCopyToLocal();
	}

	static void testCopyFromLocal() throws Exception {
		FileSystem fs = TestHdfsUtil.getFileSystem();
		fs.copyFromLocalFile(new Path("e:/4.txt"), new Path("/alonzo/api/3.txt"));
		fs.close();
	}

	static void testCopyToLocal() throws Exception {
		FileSystem fs = TestHdfsUtil.getFileSystem();
		fs.copyToLocalFile(new Path("/alonzo/api/3.txt"), new Path("e:/5.txt"));
		fs.close();
	}
}
