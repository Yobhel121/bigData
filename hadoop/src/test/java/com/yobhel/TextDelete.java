package com.yobhel;

import com.yobhel.util.TestHdfsUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class TextDelete {
	public static void main(String[] args) throws IOException {
		// testDelete();
		testDeleteOnExit();
	}

	static void testDelete() throws IOException {
		FileSystem fs = TestHdfsUtil.getFileSystem();
		boolean deleted = fs.delete(new Path("/alonzo/api/1.txt"), true);
		System.out.println(deleted);
		deleted = fs.delete(new Path("/alonzo/api/mkdirs"), false);
		System.out.println(deleted);
		deleted = fs.delete(new Path("/alonzo/api"), false);
		System.out.println(deleted);
		fs.close();
	}

	static void testDeleteOnExit() throws IOException {
		FileSystem fs = TestHdfsUtil.getFileSystem();
		boolean deleted = fs.delete(new Path("/alonzo/api/1.txt"), true);
//		boolean deleted = fs.delete(new Path("/alonzo/api/createNewFile1.txt"), true);
		System.out.println("Delete方法删除" + deleted);
		deleted = fs.deleteOnExit(new Path("/alonzo/api/2.txt"));
//		deleted = fs.deleteOnExit(new Path("/alonzo/api/3.txt"));
		System.out.println("DeleteOnExit方法删除:" + deleted);
		System.in.read();
		fs.close();
	}
}
