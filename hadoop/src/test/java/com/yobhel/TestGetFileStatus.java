package com.yobhel;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.yobhel.util.TestHdfsUtil;

public class TestGetFileStatus {
	public static void main(String[] args) throws Exception{
		FileSystem fs = TestHdfsUtil.getFileSystem();
		FileStatus status = fs.getFileStatus(new Path("/alonzo/api/3.txt"));
		System.out.println(status.isDirectory() ? "是文件夹" : "是文件");
		System.out.println("提交时间:" + status.getAccessTime());
		System.out.println("复制因子:" + status.getReplication());
		System.out.println("长度:" + status.getLen());
		System.out.println("最后修改时间:" + status.getModificationTime());
		fs.close();
	}
}
