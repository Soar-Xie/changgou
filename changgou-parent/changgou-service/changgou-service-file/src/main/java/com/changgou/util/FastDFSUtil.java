package com.changgou.util;

import com.changgou.file.FastDFSFile;
import org.csource.common.MyException;
import org.csource.common.NameValuePair;
import org.csource.fastdfs.*;
import org.springframework.core.io.ClassPathResource;

import java.io.*;

/**
 * 实现文件上传、删除下载、信息获取、storage信息获取、tracker信息获取
 */
public class FastDFSUtil {

    /**
     *加载tracker连接信息
     */
    static {
        try {
            String filename = new ClassPathResource("fdfs_client.conf").getPath();
            ClientGlobal.init(filename);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (MyException e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件上传
     *
     * @param fastDFSFile 上传的文件信息封装
     */
    public static String[] upload(FastDFSFile fastDFSFile) throws Exception {
        //附加参数
        NameValuePair[] mata_list = new NameValuePair[1];
        mata_list[0] = new NameValuePair("author", fastDFSFile.getAuthor());

        //获取TrackerServer
        TrackerServer trackerServer = getTrackerServer();

        //获取StorageClient
        StorageClient storageClient = getStorageClient(trackerServer);

        //通过StorageClient访问storage，实现文件上传，并且获得文件上传后的存储信息
        /**
         * 1.上传文件的字节数组
         * 2.文件的扩展名 jpg
         * 3.附加参数， 比如拍摄地址：北京
         *
         * 返回值upload[];
         * upload[0]：文件上传存储的storage的组名字 group1
         * upload[1]：文件存储到storage的文件名字， M00/02/44/itheima.jpg
         */
        String[] uploads = storageClient.upload_file(fastDFSFile.getContent(), fastDFSFile.getExt(), mata_list);
        return uploads;
    }

    /**
     * 获取文件信息
     *
     * @param groupName      文件组名 group1
     * @param remoteFileName 文件的存储路径名字 M00/00/00/wKjThF0DBzaAP23MAAXz2mMp9oM26.jpeg
     * @return
     */
    public static FileInfo getFile(String groupName, String remoteFileName) throws Exception {
        //获取TrackerServer
        TrackerServer trackerServer = getTrackerServer();

        //获取StorageClient
        StorageClient storageClient = getStorageClient(trackerServer);
        return storageClient.get_file_info(groupName, remoteFileName);
    }

    /**
     * 文件下载
     *
     * @param groupName
     * @param remoteFileName
     * @throws Exception
     */
    public static InputStream downloadFile(String groupName, String remoteFileName) throws Exception {
        //获取TrackerServer
        TrackerServer trackerServer = getTrackerServer();

        //获取StorageClient
        StorageClient storageClient = getStorageClient(trackerServer);

        //文件下载
        byte[] buffer = storageClient.download_file(groupName, remoteFileName);
        return new ByteArrayInputStream(buffer);
    }

    /**
     * 文件删除
     * @param groupName
     * @param remoteFileName
     * @throws Exception
     */
    public static void deleteFile(String groupName, String remoteFileName) throws Exception {
        //获取TrackerServer
        TrackerServer trackerServer = getTrackerServer();

        //获取StorageClient
        StorageClient storageClient = getStorageClient(trackerServer);

        storageClient.delete_file(groupName, remoteFileName);
    }

    /**
     * 获取storage信息
     *
     * @return
     * @throws Exception
     */
    public static StorageServer getStorage() throws Exception {
        //创建一个Tracker访问的客户端对象TrackerClient
        TrackerClient trackerClient = new TrackerClient();

        //通过TrackerClient访问TrackerSever服务，获取连接信息
        TrackerServer trackerServer = trackerClient.getConnection();
        return trackerClient.getStoreStorage(trackerServer);
    }

    /**
     * 获取storage信息
     * @param groupName
     * @param remoteFileName
     * @return
     * @throws Exception
     */
    public static ServerInfo[] getServerInfo(String groupName, String remoteFileName) throws Exception {
        //创建一个Tracker访问的客户端对象TrackerClient
        TrackerClient trackerClient = new TrackerClient();

        //通过TrackerClient访问TrackerSever服务，获取连接信息
        TrackerServer trackerServer = trackerClient.getConnection();

        return trackerClient.getFetchStorages(trackerServer, groupName, remoteFileName);
    }

    /**
     * 获取tracker信息
     *
     * @return
     */
    public static String getTrackInfo() throws Exception {
        //获取TrackerServer
        TrackerServer trackerServer = getTrackerServer();

        //Tracker的ip，http端口
        String ip = trackerServer.getInetSocketAddress().getHostString();
        int tracker_http_port = ClientGlobal.getG_tracker_http_port();
        String url = "http://" + ip + ":" + tracker_http_port;
        return url;
    }

    public static TrackerServer getTrackerServer() throws Exception {
        //创建一个Tracker访问的客户端对象TrackerClient
        TrackerClient trackerClient = new TrackerClient();

        //通过TrackerClient访问TrackerSever服务，获取连接信息
        TrackerServer trackerServer = trackerClient.getConnection();
        return trackerServer;
    }

    public static StorageClient getStorageClient(TrackerServer trackerServer) {
        StorageClient storageClient = new StorageClient(trackerServer, null);
        return storageClient;
    }
    public static void main(String[] args) throws Exception {
//        FileInfo fileInfo = getFile("group1", "M00/00/00/wKiZCmAkkGuAX3wBAADleiGOiiI944.jpg");
//        System.out.println(fileInfo.getSourceIpAddr());
//        System.out.println(fileInfo.getFileSize());
        //文件下载
       /* InputStream is = downloadFile("group1", "M00/00/00/wKiZCmAkkGuAX3wBAADleiGOiiI944.jpg");

        //将文件写入到本地磁盘
        FileOutputStream os = new FileOutputStream("D:/1.jpg");

        //定义一个缓冲区
        byte[] buffer = new byte[1024];
        while (is.read(buffer) != -1) {
            os.write(buffer);
        }
        os.flush();
        os.close();
        is.close();*/

        //文件删除
//        deleteFile("group1", "M00/00/00/wKiZCmAkwmiAIq4lAAE_7fiRlWk974.jpg");
//        StorageServer storage = getStorage();
//        System.out.println(storage.getStorePathIndex());
//        System.out.println(storage.getInetSocketAddress());
//        System.out.println(storage.getSocket());

        //获取storage组ip和端口信息
//        ServerInfo[] groups = getServerInfo("group1", "M00/00/00/wKiZCmAjtYeAKSktAAE_7fiRlWk290.jpg");
//        for (ServerInfo group : groups) {
//            System.out.println(group.getIpAddr());
//            System.out.println(group.getPort());
//        }

        //获取tracker的信息
        System.out.println(getTrackInfo());
    }
}
