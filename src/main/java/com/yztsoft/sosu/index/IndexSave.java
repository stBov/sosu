package com.yztsoft.sosu.index;

/**
 * @classname: IndexSave
 * @description:
 * @author: Shi Shijie
 * @create: 2018-11-22 14:44
 **/

public class IndexSave {
    /*// 创建仓库
    private void createSnapshotRepository(String repositoryName) {
        Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch_csi")
                .put("client.transport.sniff", true).put("location", "/mount/backups/newrepo").put("compress","true").build();
        PutRepositoryRequestBuilder putRepo =new PutRepositoryRequestBuilder(EsClient.CLIENT.admin().cluster());
        PutRepositoryResponse resp = putRepo.setName(repositoryName)
                .setType("fs")
                .setSettings(settings)
                .execute().actionGet();
        if(resp.isAcknowledged()){
            logger.info("创建仓库成功");
        }else{
            logger.info("创建仓库失败");
        }

    }

    public static  void deleteRepository(String repositoryName) {
        DeleteRepositoryRequestBuilder builder =
                new DeleteRepositoryRequestBuilder(EsClient.CLIENT.admin().cluster());
        builder.setName(repositoryName);
        DeleteRepositoryResponse resp = builder.execute().actionGet();
        if(resp.isAcknowledged()){
            logger.info("删除仓库成功");
        }else{
            logger.info("删除仓库失败");
        }

    }
    //　创建快照
    public void createSnapshot(String repositoryName, String snapshotPrefix, String indices) {
        CreateSnapshotRequestBuilder builder = new CreateSnapshotRequestBuilder(EsClient.CLIENT.admin().cluster());
        String snapshot = snapshotPrefix + "_" +UtilDate.getFormatDate("yyyy_MM_dd");
        builder.setRepository(repositoryName)
                .setIndices(indices)
                .setSnapshot(snapshot);
        CreateSnapshotResponse resp = builder.execute().actionGet();
        if(resp.status() != RestStatus.INTERNAL_SERVER_ERROR  ){
            logger.info("创建快照成功");
        }else{
            logger.info("创建快照失败");
        }
    }
    // 删除快照
    public static void deleteSnapshot(String repositoryName, String snapshot) {
        DeleteSnapshotRequestBuilder builder = new DeleteSnapshotRequestBuilder(getInstance().admin().cluster());
        builder.setRepository(repositoryName).setSnapshot(snapshot);
        DeleteSnapshotResponse resp =  builder.execute().actionGet();

        if(resp.isAcknowledged() ){
            logger.info("删除快照成功");
        }else{
            logger.info("删除快照失败");
        }
    }
    // 恢复快照
    public static void restoreSnapshot(String repositoryName, String snapshot) {
        // Obtain the snapshot and check the indices that are in the snapshot
        GetSnapshotsRequestBuilder builder = new GetSnapshotsRequestBuilder(getInstance().admin().cluster());
        builder.setRepository(repositoryName);
        builder.setSnapshots(snapshot);
        GetSnapshotsResponse resp = builder.execute().actionGet();

        // Check if the index exists and if so, close it before we can restore it.
        ImmutableList indices = resp.getSnapshots().get(0).indices();
        CloseIndexRequestBuilder closeIndexRequestBuilder =
                new CloseIndexRequestBuilder(client.admin().indices());
        closeIndexRequestBuilder.setIndices((String[]) indices.toArray(new String[indices.size()]));
        closeIndexRequestBuilder.execute().actionGet();
        // Now execute the actual restore action
        RestoreSnapshotRequestBuilder restoreBuilder = new RestoreSnapshotRequestBuilder(getInstance().admin().cluster());
        restoreBuilder.setRepository(repositoryName).setSnapshot(snapshot);
        RestoreSnapshotResponse resp1 =  restoreBuilder.execute().actionGet();
        if(resp1.status() !=  RestStatus.INTERNAL_SERVER_ERROR ){
            logger.info("恢复快照成功");
            for(String list : (String[]) indices.toArray(new String[indices.size()])){
                logger.info("恢复的快照名为:" + list);
            }
        }else{
            logger.info("恢复快照失败");
        }
    }
*/

}
