package com.test.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class HDFSTest {
    public static void main(String[] args) throws IOException {
//        Files.copy(Paths.get("./input/word"), getOutputStream("/input/word"));
//        createFile("/test", bytes);
//        delete("/output/word");
//        getDataNode();
        getDir("/output/word");
//        copyFromLocalFile("C:\\Users\\Administrator\\Desktop\\loan-qa.sql", "/test-data/loan-sql");
//        getFile("/output/word");
//        fileLocation("/output/word");
    }


    public static void createFile(String dest, byte[] contents) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(dest);
        FSDataOutputStream outputStream = fileSystem.create(path);
        outputStream.write(contents);
        outputStream.close();
        fileSystem.close();
        System.out.println("文件创建成功");
    }

    public static FSDataOutputStream getOutputStream(String dest) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(dest);
        FSDataOutputStream outputStream = fileSystem.create(path);
        return outputStream;
    }

    public static void delete(String dest) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(dest);
        fileSystem.deleteOnExit(path);
    }

    public static void fileLocation(String name) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        FileStatus fs = fileSystem.getFileStatus(new Path(name));
        BlockLocation[] bls = fileSystem.getFileBlockLocations(fs, 0, fs.getLen());
        for (int i = 0, h = bls.length; i < h; i++) {
            String[] hosts = bls[i].getHosts();
            System.out.println("block_" + i + "_location:  " + hosts[0]);
        }
    }

    public static void getDataNode() throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        DistributedFileSystem hdfs = (DistributedFileSystem) fileSystem;
        DatanodeInfo[] dns = hdfs.getDataNodeStats();
        for (int i = 0, h = dns.length; i < h; i++) {
            System.out.println("datanode_" + i + "_name:  " + dns[i].getHostName());
        }
    }

    public static void copyFromLocalFile(String src, String dst) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path srcPath = new Path(src);
        Path dstPath = new Path(dst);
        fileSystem.copyFromLocalFile(srcPath, dstPath);
        System.out.println("文件拷贝成功！");
    }

    public static void getFile(String file) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(file);
        FSDataInputStream in = null;
        try {
            in = fileSystem.open(path);
            IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
        } finally {
            IOUtils.closeStream(in);
        }
    }

    public static void getDir(String dir) throws IOException {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://192.168.154.135:9000");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path path = new Path(dir);
        InputStream in = null;
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, false);
        while (iterator.hasNext()) {
            try {
                LocatedFileStatus fileStatus = iterator.next();
                Path p = fileStatus.getPath();
                BlockLocation[] bls = fileSystem.getFileBlockLocations(p, 0, fileStatus.getLen());
                for (int i = 0, h = bls.length; i < h; i++) {
                    String[] hosts = bls[i].getHosts();
                    System.out.println("block_" + i + "_location:  " + hosts[0]);
                }
                in = fileSystem.open(p);
                System.out.println(p);
                IOUtils.copyBytes(in, System.out, 4096, false); //复制到标准输出流
            } finally {
                IOUtils.closeStream(in);
            }
        }
    }
}
