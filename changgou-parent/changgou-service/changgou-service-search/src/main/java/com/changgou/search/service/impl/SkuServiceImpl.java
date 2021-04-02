package com.changgou.search.service.impl;

import com.alibaba.fastjson.JSON;
import com.changgou.goods.feign.SkuFeign;
import com.changgou.goods.pojo.Sku;
import com.changgou.search.dao.SkuEsMapper;
import com.changgou.search.pojo.SkuInfo;
import com.changgou.search.service.SkuService;
import entity.Result;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchResultMapper;
import org.springframework.data.elasticsearch.core.aggregation.AggregatedPage;
import org.springframework.data.elasticsearch.core.aggregation.impl.AggregatedPageImpl;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.util.*;

@Service
public class SkuServiceImpl implements SkuService {

    @Autowired
    private SkuFeign skuFeign;

    @Autowired
    private SkuEsMapper skuEsMapper;
    /**
     * 导入数据到数据库中
     */
    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    /**
     * 多条件搜索
     *
     * @param searchMap
     * @return
     */
    @Override
    public Map<String, Object> search(Map<String, String> searchMap) {
        //准备查询条件 select * from table-->Resultset
        //sql语句
        //循环ResultSet->javaBean->List<JavaBean>
        //搜索条件封装
        //String sql = "select * from tb_sku where name like ";
        //等价于||西面的搜索
        NativeSearchQueryBuilder nativeSearchQueryBuilder = buildBasicQuery(searchMap);


        //集合搜索
        Map<String, Object> resultMap = searchList(nativeSearchQueryBuilder);

        //用户选择分类后就不用显示分类分组
        if (searchMap == null || StringUtils.isEmpty(searchMap.get("category"))) {
            //分类分组查询
            List<String> categoryList = searchCategoryList(nativeSearchQueryBuilder);
            resultMap.put("categoryList", categoryList);
        }

        //用户选择品牌，就不用显示品牌选择
        if (searchMap == null || StringUtils.isEmpty(searchMap.get("brand"))) {
            List<String> brandList = searchBrandList(nativeSearchQueryBuilder);
            resultMap.put("brandList", brandList);
        }
        //规格查询
        Map<String, Set<String>> specList = searchSpecList(nativeSearchQueryBuilder);
        resultMap.put("specList", specList);

        return resultMap;
    }

    /**
     * 搜索条件封装
     *
     * @param searchMap
     * @return
     */
    private NativeSearchQueryBuilder buildBasicQuery(Map<String, String> searchMap) {
        //NativeSearchBuilder,搜索条件构造对象
        NativeSearchQueryBuilder nativeSearchQueryBuilder = new NativeSearchQueryBuilder();
        //boolQuery 组合条件 must，must_not,should
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();

        //判断条件
        if (searchMap != null && searchMap.size() > 0) {
            String keywords = searchMap.get("keywords");
            if (!StringUtils.isEmpty(keywords)) {
//                nativeSearchQueryBuilder.withQuery(QueryBuilders.queryStringQuery(keywords).field("name"));
                boolQueryBuilder.must(QueryBuilders.queryStringQuery(keywords).field("name"));
            }
            //输入了分类
            if (!StringUtils.isEmpty(searchMap.get("category"))) {
                boolQueryBuilder.must(QueryBuilders.termQuery("categoryName", searchMap.get("category")));

            }

            //输入了品牌
            if (!StringUtils.isEmpty(searchMap.get("brand"))) {
                boolQueryBuilder.must(QueryBuilders.termQuery("brandName", searchMap.get("brand")));
            }
            //规格过滤实现
            for (Map.Entry<String, String> entry : searchMap.entrySet()) {
                String key = entry.getKey();
                //如果key以spec_开始，则开始筛选查询
                if (key.startsWith("spec_")) {
                    //规格条件的值
                    String value = entry.getValue();
                    //spec_网络，spec_前五个去掉
                    boolQueryBuilder.must(QueryBuilders.termQuery("specMap." + key.substring(5) + ".keyword", value));
                }
            }
        }
        //price，去点元和以上
        //根据-分割,x一定b
//        String price = searchMap.get("price");
//        if (!StringUtils.isEmpty(true)) {
//            price = price.replace("元", "").replace("以上", "");
//            //分割
//            String[] prices = price.split("-");
//            if (prices != null && price.length() > 0) {
//                boolQueryBuilder.must(QueryBuilders.rangeQuery("price").gt(Integer.parseInt(prices[0])));
//                if (prices.length == 2) {
//                    boolQueryBuilder.must(QueryBuilders.rangeQuery("price").lte(Integer.parseInt(prices[1])));
//                }
//            }
//        }

        //排序实现
        String sortField = searchMap.get("sortField");  //指定排序域
        String sortRule = searchMap.get("sortRule");    //指定排序规则
        if (!StringUtils.isEmpty(sortField) && !StringUtils.isEmpty(sortRule)) {
            nativeSearchQueryBuilder.withSort(new FieldSortBuilder(sortField).order(SortOrder.valueOf(sortRule)));
            //指定排序域与规则
        }


        //将boolQueryBuilder填充给naticeSearchBuilder

        //分页，默认在第一页
        Integer pageNum = coverterPage(searchMap);
        Integer size = 3;   //每页显示条数
        nativeSearchQueryBuilder.withPageable(PageRequest.of(pageNum - 1, size));


        nativeSearchQueryBuilder.withQuery(boolQueryBuilder);
        return nativeSearchQueryBuilder;
    }

    /**
     * 接受前端参数
     *
     * @param searchMap
     * @return
     */
    public Integer coverterPage(Map<String, String> searchMap) {
        //动态获取页面数据
        if (searchMap != null) {
            String pageNum = searchMap.get("pageNum");
            try {
                return Integer.parseInt(pageNum);//返回数值类型的页码
            } catch (NumberFormatException e) {
                e.printStackTrace();
            }
        }
        return 1;   //默认返回第一页
    }

    private Map<String, Object> searchList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        //配置高亮
        HighlightBuilder.Field field = new HighlightBuilder.Field("name");    //设置高亮域
        //前缀<em style="color:red">
        field.preTags("em style=\"color:red;\">");
        //后缀
        field.postTags("</em>");
        //碎片长度
        field.fragmentSize(100);    //会通过前缀与后缀进行分割
        //添加高亮
        nativeSearchQueryBuilder.withHighlightFields(field);
        //执行搜索，得到响应结果,1.搜搜条件封装对象2.搜索的结果集合
        //返回值时搜索集的封装
//        AggregatedPage<SkuInfo> page = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        //执行查询，获取所有数据->高亮与非高亮
        AggregatedPage<SkuInfo> page = elasticsearchTemplate
                .queryForPage(
                        nativeSearchQueryBuilder.build(),   //搜索条件封装
                        SkuInfo.class,  //数据集合要转换的类型的字节码
                        new SearchResultMapper() {
                            @Override
                            public <T> AggregatedPage<T> mapResults(SearchResponse response, Class<T> aClass, Pageable pageable) {
                                //存储转换后的高亮数据对象
                                List<T> list = new ArrayList<>();
                                //执行查询，获取所有数据
                                for (SearchHit hit : response.getHits()) {
                                    //找出非高亮数据
                                    SkuInfo skuInfo = JSON.parseObject(hit.getSourceAsString(), SkuInfo.class);
                                    //找出高亮数据,某个域的
                                    HighlightField highlightField = hit.getHighlightFields().get("name");
                                    if (highlightField !=null && highlightField.getFragments() !=null){
                                        //高亮数据读取出来
                                        Text[] fragments = highlightField.getFragments();
                                        StringBuffer buffer = new StringBuffer();
                                        for (Text fragment : fragments) {
                                            buffer.append(fragment.toString());
                                        }
                                        //非高亮数据中指定的域替换成高亮数据
                                        skuInfo.setName(buffer.toString());
                                    }
                                    //将高亮数据添加到集合中
                                    list.add((T) skuInfo);
                                }
                                //将数据返回 1.搜索的集合数据 content 分页对象信息:Pageable pageable 总记录数 long total
                                return new AggregatedPageImpl<>(list,pageable,response.getHits().getTotalHits());
                            }
                        });

        //分页参数-总记录数
        long totalElements = page.getTotalElements();

        //总记录数
        int totalPages = page.getTotalPages();

        //获取数据结果集
        List<SkuInfo> contents = page.getContent();

        //封装一个Map存储所有数据，并返回
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("rows", contents);
        resultMap.put("total", totalElements);
        resultMap.put("totalPages", totalPages);
        return resultMap;
    }

    /**
     * 分类分组查询
     *
     * @param nativeSearchQueryBuilder
     * @return
     */
    private List<String> searchCategoryList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        /**
         * 分组查询分类集合
         * .addAggregation():添加一个聚合操作
         1.取别名，确定域
         */
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuCategory").field("categoryName"));
        AggregatedPage<SkuInfo> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        //获取分组数据
        StringTerms stringTerms = aggregatedPage.getAggregations().get("skuCategory");
        List<String> categoryList = new ArrayList<String>();
        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            String categoryName = bucket.getKeyAsString(); //其中的一个分类名字
            categoryList.add(categoryName);
        }
        return categoryList;
    }

    /**
     * 品牌分组查询
     *
     * @param nativeSearchQueryBuilder
     * @return
     */
    private List<String> searchBrandList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        /**
         * 分组查询分类集合
         * .addAggregation():添加一个聚合操作
         1.取别名，确定域
         */
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuBrand").field("brandName"));

        AggregatedPage<SkuInfo> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        //获取分组数据,.get("skuBrand"):获取指定域的集合数
        StringTerms stringTerms = aggregatedPage.getAggregations().get("skuBrand");
        List<String> brandList = new ArrayList<String>();
        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            String brandName = bucket.getKeyAsString(); //其中的一个分类名字
            brandList.add(brandName);
        }
        return brandList;
    }

    /**
     * 品牌分组查询
     *
     * @param nativeSearchQueryBuilder
     * @return
     */
    private Map<String, Set<String>> searchSpecList(NativeSearchQueryBuilder nativeSearchQueryBuilder) {
        /**
         * 分组查询规格集合
         * .addAggregation():添加一个聚合操作
         1.取别名，确定域
         */
        nativeSearchQueryBuilder.addAggregation(AggregationBuilders.terms("skuSpec").field("spec.keyword").size(10000));

        AggregatedPage<SkuInfo> aggregatedPage = elasticsearchTemplate.queryForPage(nativeSearchQueryBuilder.build(), SkuInfo.class);

        //获取分组数据,.get("skuSpec"):获取指定域的集合数
        StringTerms stringTerms = aggregatedPage.getAggregations().get("skuSpec");
        List<String> specList = new ArrayList<String>();
        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            String specName = bucket.getKeyAsString(); //其中的一个规格名字
            specList.add(specName);
        }
        Map<String, Set<String>> allSpec = putAllSpec(specList);

        return allSpec;
    }

    /**
     * 规格汇总合并
     *
     * @param specList
     * @return
     */
    private Map<String, Set<String>> putAllSpec(List<String> specList) {
        //合并后的map
        Map<String, Set<String>> allSpec = new HashMap<String, Set<String>>();
        //1.循环specList
        for (String spec : specList) {
            //2.将每个json字符串都转成map
            Map<String, String> specMap = JSON.parseObject(spec, Map.class);
            //3.将每个map对象合成一个map<string,set<string>>]

            //4.合并流程
            //4.1循环所有MAp
            for (Map.Entry<String, String> entry : specMap.entrySet()) {
                //4.2去除当前map，并获取key和value
                String key = entry.getKey();    //规格名字
                String value = entry.getValue();//规格值
                //4.3将当前循环的数据合并到Map中
                //从allSpec中获取当前的set集合数据
                Set<String> specSet = allSpec.get(key);
                if (specSet == null) {
                    //之前allSpec中没有该规格
                    specSet = new HashSet<String>();
                }
                specSet.add(value);
                allSpec.put(key, specSet);
            }
        }
        return allSpec;
    }

    @Override
    public void importData() {
        //Feign调用，查询List<Sku>
        Result<List<Sku>> skuResult = skuFeign.findAll();
        //将List<Sku>转成List<SkuInfo>
        List<SkuInfo> skuInfoList = JSON.parseArray(JSON.toJSONString(skuResult.getData()), SkuInfo.class);
        //调用Dao实现数据导入
        for (SkuInfo skuInfo : skuInfoList) {
            //获取spec->Map(String)->Map类型
            Map<String, Object> specMap = JSON.parseObject(skuInfo.getSpec());
            //如果需要生成动态的域，只需要将该域存入到一个Map<String,Object>对象中即可，该Map<String,Object>的key会生成一个域，域的名字为Map的key
            //当前Map<String,Object>后面object的值会作为当前sku对象域key对应的值
            skuInfo.setSpecMap(specMap);
        }
        skuEsMapper.saveAll(skuInfoList);
    }
}
