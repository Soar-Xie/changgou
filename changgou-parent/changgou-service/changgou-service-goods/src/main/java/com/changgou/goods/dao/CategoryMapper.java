package com.changgou.goods.dao;
import com.changgou.goods.pojo.Category;
import org.apache.ibatis.annotations.Select;
import tk.mybatis.mapper.common.Mapper;

import java.util.List;

/****
 * @Author:shenkunlin
 * @Description:Category的Dao
 * @Date 2019/6/14 0:12
 *****/
public interface CategoryMapper extends Mapper<Category> {
    /**
     * 根据父节id查询所有子分类
     * @param pid
     * @return
     */
   /* @Select("select tb.* from tb_brand tb,tb_category_brand tcb where tb.id=tcb.brand_id and tcb.category_id=#{pid}")
    List<Category> findByParentId(Integer pid);*/
}
