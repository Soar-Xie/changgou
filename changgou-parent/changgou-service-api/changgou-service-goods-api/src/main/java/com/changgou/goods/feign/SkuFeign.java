package com.changgou.goods.feign;

import com.changgou.goods.pojo.Sku;
import entity.Result;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@FeignClient(value = "goods")
@RequestMapping("/sku")
//@FeignClient(name="goods")
//@RequestMapping(value = "/sku")
public interface SkuFeign {

    /**
     * 查询所有数据
     * @return
     */
    @GetMapping
    Result<List<Sku>> findAll();

    /***
     * 根据审核状态查询Sku
     * @param status
     * @return
     */
    @GetMapping("/status/{status}")
    Result<List<Sku>> findByStatus(@PathVariable String status);
}
