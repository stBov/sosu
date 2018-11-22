package com.yztsoft.sosu.index;

import com.yztsoft.sosu.esclient.EsClient;
import com.yztsoft.sosu.utils.FileUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @classname: IndexService
 * @description:
 * @author: Shi Shijie
 * @create: 2018-11-22 14:32
 **/

public class IndexService {

    private static Logger logger = LogManager.getLogger(IndexService.class);
    private static final String INDEX_MAPPING="config/mappings/";
    protected String typeName;
    protected String indexName;
    public IndexService(String typeName,String indexName){
        this.typeName = typeName;
        this.indexName = indexName;
    }

    /**
     * 添加索引别名
     * @param
     * @param aliases  别名
     * void
     * @throws
     * @author miwang
     * @date 2016年10月28日 下午3:42:03
     */
    public void addAlias(String indexs,String aliases){
        IndicesAliasesRequest request=new IndicesAliasesRequest();
        request.addAlias(aliases,indexs);
        EsClient.CLIENT.admin().indices().aliases(request).actionGet();
    }

    /**
     * 删除索引别名
     * @param index 索引
     * @param aliases 别名
     * void
     * @throws
     * @author miwang
     * @date 2016年10月28日 下午3:40:05
     */
    public void removeAlias(String index,String aliases){
        IndicesAliasesRequest request=new IndicesAliasesRequest();
        request.removeAlias(index, aliases);
        EsClient.CLIENT.admin().indices().aliases(request).actionGet();
    }

    /**
     * 创建索引
     * @param
     * @throws Exception
     * void
     * @throws
     * @author miwang
     * @date 2016年4月8日 下午6:24:30
     */
    public void createIndex()throws Exception{
        logger.info("开始创建索引{}",indexName);
        if(!existsIndex()){
            // 如果索引不存在，则创建索引
            logger.info("索引{}不存在，创建索引",indexName);
            CreateIndexRequest request = new CreateIndexRequest(indexName);
            Map<String,Object> map=new HashMap<String, Object>();
            map.put("number_of_replicas","1");
            map.put("number_of_shards","3");
            map.put("max_result_window",50000);
            request.settings(map);
            CreateIndexResponse response=EsClient.CLIENT.admin().indices().create(request).actionGet();
            if (response.isAcknowledged()) {
                logger.info("成功创建索引{}",indexName);
            } else {
                logger.info("创建索引{}失败",indexName);
            }
        }
        if(!existsType()){
            createBangMapping();
        }
    }

    public void createBangMapping()throws Exception{
        String source = FileUtil.readJsonDefn(INDEX_MAPPING+typeName+".json");
        if (source != null) {
            PutMappingRequest request = Requests.putMappingRequest(indexName).type(typeName).source(source);
            PutMappingResponse resp = EsClient.CLIENT.admin().indices().putMapping(request).actionGet();
            if (resp.isAcknowledged()) {
                logger.info("成功创建索引{}类型{}Mapping",indexName,typeName);
            } else {
                logger.error("创建索引{}类型{}Mapping失败",indexName,typeName);
            }
        }else{
            logger.error("创建索引{}类型{}Mapping失败,没有对应的mapping文件",indexName,typeName);
            throw new RuntimeException(INDEX_MAPPING+typeName+".json not found");
        }

    }



    /**
     * 批量插入数据 只支持JSON 格式
     *
     * @param datas
     *            key数据ID 作为文档ID value-数据（josn 格式） void
     * @throws
     * @author miwang
     * @date 2016年4月9日 下午3:06:51
     */
    public void batchInsert(Map<String,String> datas) {
        if (datas != null && !datas.isEmpty()) {
            long now=System.currentTimeMillis();
            BulkRequestBuilder bulkRequest = EsClient.CLIENT.prepareBulk();
            for (Map.Entry<String, String> entry : datas.entrySet()) {
                bulkRequest.add(EsClient.CLIENT.prepareIndex(this.indexName, this.typeName, entry.getKey()).setSource(entry.getValue()));
            }
            BulkResponse bulkResponse = bulkRequest.get();
            if (bulkResponse.hasFailures()) {
                logger.error("syn index name:{} type name:{}  pageSie:{}  failed:{} complate time:{}",this.indexName,this.typeName,datas.size(),bulkResponse.buildFailureMessage(),(System.currentTimeMillis()-now)+"");
            }else{
                if(logger.isDebugEnabled()){
                    logger.debug("syn index name:{} type name:{}  pageSie:{} success complate time:{}",this.indexName,this.typeName,datas.size(),(System.currentTimeMillis()-now)+"");
                }
            }
            datas=null;
        }
    }

    public Boolean batchDelete(Set<String> datas) {
        if(datas==null || datas.isEmpty()){
            return false;
        }
        BulkRequestBuilder bulkRequest =EsClient.CLIENT.prepareBulk();
        for (String id : datas) {
            bulkRequest.add(EsClient.CLIENT.prepareDelete(this.indexName,this.typeName,id).request());
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            for(BulkItemResponse item : bulkResponse.getItems()){
                logger.error(item.getFailureMessage());
            }
            return false;
        }else {
            return true;
        }
    }

    public Boolean deleteDoc(String id) {
        DeleteRequestBuilder request=EsClient.CLIENT.prepareDelete();
        request.setIndex(this.indexName);
        request.setType(this.typeName);
        request.setId(id);
        request.get();
        return true;
    }

    public Boolean deleteDocByQuery(String hotelId) {
        String deletebyquery = "{\"query\": {\"match\": {\"hotelId\": \"" + hotelId + "\"}}}";
        @SuppressWarnings("unused")
        DeleteByQueryResponse response = new DeleteByQueryRequestBuilder(EsClient.CLIENT, DeleteByQueryAction.INSTANCE)
                .setIndices(this.indexName).setTypes(this.typeName).setSource(deletebyquery).execute().actionGet();
        return true;
    }



    // 更新一条文档
    public boolean update(String id,Map<String,String> data) throws InterruptedException, ExecutionException {
        UpdateRequest uRequest = new UpdateRequest();
        uRequest.index(this.indexName);
        uRequest.type(this.typeName);
        uRequest.id(id);
        uRequest.doc(data.get(id));
        EsClient.CLIENT.update(uRequest).get();
        return true;
    }


    /**
     * 删除当前索引
     * @return
     * Boolean
     * @throws
     * @author miwang
     * @date 2016年4月12日 上午11:27:10
     */
    public Boolean deleteIndex() {
        boolean rs=false;
        IndicesExistsResponse indicesExistsResponse =EsClient.CLIENT.admin().indices()
                .exists(new IndicesExistsRequest(new String[] {indexName})).actionGet();
        if (indicesExistsResponse.isExists()) {
            DeleteIndexResponse delete =EsClient.CLIENT.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet();
            if (!delete.isAcknowledged()) {
                rs=false;
                logger.error("Index " + indexName + " wasn't deleted");
            } else {
                rs=true;
                logger.info("Index " + indexName + "  deleted ok ...");
            }
        }
        return rs;
    }


    /**
     * 判断当前索引是否存在
     * @return
     * boolean
     * @throws
     * @author miwang
     * @date 2016年4月12日 上午11:26:37
     */
    public boolean existsIndex() {
        return (EsClient.CLIENT.admin().indices().prepareExists(indexName).execute().actionGet().isExists());
    }

    /**
     * 判断当前文档类型是否存在
     * @return
     * boolean
     * @throws
     * @author miwang
     * @date 2016年4月12日 上午11:26:17
     */
    public boolean existsType() {
        TypesExistsResponse response = EsClient.CLIENT.admin().indices().prepareTypesExists(indexName).setTypes(typeName).execute()
                .actionGet();
        return response.isExists();
    }

    /**
     * 判断文档是否存在
     * @param id 文档ID
     * @return
     * boolean
     * @throws
     * @author miwang
     * @date 2016年4月12日 上午11:25:54
     */
    public boolean existsDoc(String id){
        return StringUtils.isNotBlank(findById(id));
    }

    public String findById(String id){
        SearchResponse resp =EsClient.CLIENT.prepareSearch(indexName).setTypes(typeName)
                .setQuery(QueryBuilders.idsQuery().ids(id))
                .setSize(1).execute().actionGet();
        SearchHit[] items=resp.getHits().getHits();
        if(items!=null && items.length>0) {
            return items[0].getSourceAsString();
        } else{
            return null;
        }
    }



}
