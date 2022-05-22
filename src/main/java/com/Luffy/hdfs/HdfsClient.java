package com.Luffy.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;

/**
 * @Author Luffy5522
 * @date: 2022/5/3 20:58
 * @description: 客户端代码常见套路
 * 1.获取一个客户端对象
 * 2.执行相关的操作命令
 * 3.关闭资源
 */
public class HdfsClient {

    private FileSystem fs;

    @Before
    public void init() throws URISyntaxException, IOException, InterruptedException {
        // 连接的集群nn地址
        URI uri = new URI("hdfs://hadoop101:8020");
        // 创造一个配置文件
        Configuration configuration = new Configuration();

        String user = "Luffy";
        fs = FileSystem.get(uri, configuration, user);
    }

    @After
    public void close() {
        try {
            fs.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    @Test
    // 创建文件夹
    public void testMkdir() throws IOException, URISyntaxException, InterruptedException {
        // 创建一个文件夹
        fs.mkdirs(new Path("/xiyou/huaguoshan1"));
    }

    @Test
    // 文件上传
    public void testPut() throws IOException {
        // 参数一:Boolean delsrc 是否删除原数据
        // 参数二:是否运行覆盖
        // 参数三 原数据路径
        // 参数四:目的路径
        fs.copyFromLocalFile(false, false, new Path("D:\\sunwukong.txt"), new Path("/xiyou/huaguoshan"));

    }

    @Test
    // 获取文件
    public void testGet() throws IOException {
        fs.copyToLocalFile(false, new Path("/xiyou/huaguoshan/sunwukong.txt"),
                new Path("D://sunwukong1.txt"), false);
    }

    @Test
    public void testRM() throws IOException {
        // 参数一:路径 参数二:是否递归删除
        fs.delete(new Path("/xiyou/huaguoshan/sunwukkong.txt"), false);
    }

    @Test
    // 移动和重命名
    public void testMV() throws IOException {
        // 源文件路径,目标文件路径
        fs.rename(new Path("/xiyou/word.txt"), new Path("/xiyou/ss.txt"));
    }

    @Test
    // 获取文件详细信息
    public void testDetail() throws IOException {
        // 路径,是否递归
        // 获取所有文件信息
        RemoteIterator<LocatedFileStatus> llRI = fs.listFiles(new Path("/"), true);
        while (llRI.hasNext()) {
            LocatedFileStatus next = llRI.next();
            System.out.println("----" + next.getPath() + "----");
            System.out.println(next.getPermission());
            System.out.println(next.getOwner());
            System.out.println(next.getLen());
            System.out.println(next.getModificationTime());
            System.out.println(next.getReplication());
            System.out.println(next.getBlockLocations());
            System.out.println(next.getPath().getName());

            // 获取块信息
            BlockLocation[] blockLocations = next.getBlockLocations();
            System.out.println(Arrays.toString(blockLocations));
        }


    }

    // 判断是文件还是目录
    @Test
    public void testFile() throws IOException {
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for(FileStatus status :listStatus) {
            if (status.isFile()) {
                System.out.println("文件 " + status.getPath().getName());
            }else {
                System.out.println("目录 " + status.getPath().getName());
            }
        }
    }
}


