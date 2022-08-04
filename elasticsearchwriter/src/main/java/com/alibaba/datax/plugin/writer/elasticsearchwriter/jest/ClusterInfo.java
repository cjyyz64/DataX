package com.alibaba.datax.plugin.writer.elasticsearchwriter.jest;

import com.google.gson.Gson;
import io.searchbox.action.AbstractAction;
import io.searchbox.client.config.ElasticsearchVersion;

/**
 * @author yiyi adam.zwj@alibaba-inc.com
 * @version 2020
 * @date 2020-02-19
 *
 */
public class ClusterInfo extends AbstractAction<ClusterInfoResult> {
	@Override
	protected String buildURI(ElasticsearchVersion elasticsearchVersion) {
		return "";
	}

	@Override
	public String getRestMethodName() {
		return "GET";
	}

	@Override
	public ClusterInfoResult createNewElasticSearchResult(String responseBody, int statusCode, String reasonPhrase, Gson gson) {
		return createNewElasticSearchResult(new ClusterInfoResult(gson), responseBody, statusCode, reasonPhrase, gson);
	}

	public static class Builder extends AbstractAction.Builder<ClusterInfo, ClusterInfo.Builder> {

		public Builder() {
			setHeader("accept", "application/json");
			setHeader("content-type", "application/json");
		}

		@Override
		public ClusterInfo build() {
			return new ClusterInfo();
		}
	}
}
