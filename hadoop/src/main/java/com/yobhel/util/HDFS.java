package com.yobhel.util;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.InputStream;
import java.net.URI;

public class HDFS {

    /*************************************************************************
     * HDFS Utils
     * exists : 路径是否存在
     * createFile ： 创建文件
     * createFile ： 创建文件
     * copyFromLocalFile ： 上传本地文件
     * deleteFile : 删除目录或文件
     * deleteFile : 删除目录或文件(如果有子目录,则级联删除)
     * renameFile : 文件重命名
     * mkDir : 创建目录
     * listDir ： 列出指定路径下的所有文件(不包含目录)
     * listFile ： 列出指定路径下的文件(非递归)
     * listFile ： 列出指定目录下的文件\子目录信息(非递归)
     * readFile : 读取文件内容
     *
     ************************************************************************/

    private Configuration conf = new Configuration();
    private FileSystem fs = null;

    public static String default_address = "hdfs://localhost:9000";
    public static String default_username = "yezhimin";

    public HDFS() throws Exception {
        fs = FileSystem.get(new URI(default_address), conf, default_username);
    }

    public HDFS(String address, String username) throws Exception {
        fs = FileSystem.get(new URI(address), conf, username);
    }

    public boolean Exists(String dir) throws Exception {
        return fs.exists(new Path(dir));
    }

    /**
     * 列出指定目录下所有文件 包含目录
     *
     * @param path
     * @throws Exception
     */
    public void listDir(String path) throws Exception {
        FileStatus[] files = fs.listStatus(new Path(path));
        for (FileStatus f : files) {
            System.out.println(f);
        }
    }

    /**
     * 列出指定目录下所有文件 不包含目录 非递归
     *
     * @param path
     * @throws Exception
     */
    public void listFile(String path, boolean recursive) throws Exception {
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(path), recursive);
        while (itr.hasNext()) {
            System.out.println(itr.next());
        }
    }

    /**
     * 列出指定目录下所有文件 不包含目录 递归
     *
     * @param path
     * @throws Exception
     */
    public void listFile(String path) throws Exception {
        RemoteIterator<LocatedFileStatus> itr = fs.listFiles(new Path(path), false);
        while (itr.hasNext()) {
            System.out.println(itr.next());
        }
    }


    /**
     * 创建文件
     *
     * @param path
     * @param contents
     * @throws Exception
     */
    public void createFile(String path, byte[] contents) throws Exception {
        FSDataOutputStream f = fs.create(new Path(path));
        f.write(contents);
        f.close();
    }

    public void createFile(String filePath, String contents) throws Exception {
        createFile(filePath, contents.getBytes());
    }


    public void copyFromLocalFile(String src, String dst) throws Exception {
        fs.copyFromLocalFile(new Path(src), new Path(dst));
    }

    //mkdir
    public boolean mkdir(String dir) throws Exception {
        boolean isSuccess = fs.mkdirs(new Path(dir));
        return isSuccess;
    }

    //rm
    public boolean delete(String filePath, boolean recursive) throws Exception {
        boolean isSuccess = fs.delete(new Path(filePath), recursive);
        return isSuccess;
    }

    //rm -r
    public boolean delete(String filePath) throws Exception {
        boolean isSuccess = fs.delete(new Path(filePath), true);
        return isSuccess;
    }

    public void rename(String oldName, String newName) throws Exception {
        fs.rename(new Path(oldName), new Path(newName));
    }

    //
    public String readFile(String filePath) throws Exception {
        String content = null;
        InputStream in = null;
        ByteArrayOutputStream out = null;

        try {
            in = fs.open(new Path(filePath));
            out = new ByteArrayOutputStream(in.available());
            IOUtils.copyBytes(in, out, conf);
            content = out.toString();
        } finally {
            IOUtils.closeStream(in);
            IOUtils.closeStream(out);
        }

        return content;
    }


    public static void main(String[] args) throws Exception {
        HDFS dfs = new HDFS();
        String dir = "/data/hadoop";

        //查找目录
        if (!dfs.Exists(dir)) {
            System.out.println("目录不存在，正在准备创建！");
            dfs.mkdir(dir);
            dfs.mkdir("/data/hadoop/hive");
            dfs.mkdir("/data/hadoop/pig");
            dfs.mkdir("/data/hadoop/mahout");
            System.out.println("创建目录成功！");
        } else {
            System.out.println("目录已存在！文件如下：");
            dfs.listFile(dir);
            System.out.println("目录已存在！目录或文件如下：");
            dfs.listDir(dir);
        }

        //查找文件
        String filePath = "/data/hadoop/test";
        if (!dfs.Exists(filePath)) {
            System.out.println("文件不存在，正在准备创建！");
            dfs.createFile(filePath, "I love you hadoop!");
            System.out.println("创建文件成功，内容为：" + dfs.readFile(filePath));
        } else {
            System.out.println("文件已存在，正在准备删除！");
            if (dfs.delete(filePath)) {
                System.out.println("已删除文件" + filePath);
            }
            if (dfs.delete(filePath.substring(0, 12))) {
                System.out.println("已删除目录" + filePath.substring(0, 12));
            }
        }

    }
}


