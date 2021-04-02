package com.changgou.controller;

import com.changgou.file.FastDFSFile;
import com.changgou.util.FastDFSUtil;
import entity.Result;
import entity.StatusCode;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

@RestController
@RequestMapping(value = "/upload")
@CrossOrigin
public class FileUploadController {

    /**
     * 文件上传
     *
     * @return
     */
    @PostMapping
    public Result upload(@RequestParam(value = "file") MultipartFile file) throws Exception {
        //封装文件信息
        FastDFSFile fastDFSFile = new FastDFSFile(
                file.getOriginalFilename(),//文件名字
                file.getBytes(),//文件字节数组
                StringUtils.getFilenameExtension(file.getOriginalFilename()) //获取文件扩展名
        );
        //调用FastUtil工具类将文件上传到FastDFS中
        String[] uploads = FastDFSUtil.upload(fastDFSFile);
        //拼接访问地址url=http//192.168.153.10:8080/group1/M00/00/00/wKjThF0DBzaAP23MAAXz2mMp9oM26.jpeg
//        String url = "http://192.168.153.10:8080/" + uploads[0] + "/" + uploads[1];
        String url = FastDFSUtil.getTrackInfo() + "/" + uploads[0] + "/" + uploads[1];
        return new Result(true, StatusCode.OK, "上传成功", url);
    }
}
