package com.yztsoft.sosu;

import com.yztsoft.sosu.esclient.EsClient;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SosuApplicationTests {

	@Test
	public void contextLoads() {
	}

	@Test
	public void testEsClient_Aliases() {
		IndicesAliasesRequest request=new IndicesAliasesRequest();
		request.addAlias("123","test");
		EsClient.CLIENT.admin().indices().aliases(request).actionGet();
	}
}
