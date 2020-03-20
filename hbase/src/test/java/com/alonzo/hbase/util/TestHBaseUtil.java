package com.alonzo.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class TestHBaseUtil {
	/**
	 * 获取hbase的配置文件信息
	 * 
	 * @return
	 */
	public static Configuration getHBaseConfiguration() {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.2.100");
		System.setProperty("hadoop.home.dir", "E:\\大数据（全）\\大数据软件工具\\hadoop-2.5.0-cdh5.3.6\\hadoop-2.5.0-cdh5.3.6");
		return conf;
	}
}
