package com.alibaba.datax.plugin.writer.oceanbasewriter.utils;

import com.alibaba.datax.plugin.writer.oceanbasewriter.strategy.Context;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.sql.ResultSet;
import java.util.List;
import java.util.Properties;

public class ActiveMemPercentChecker {
	private static final Logger log = LoggerFactory.getLogger(ActiveMemPercentChecker.class);
	private static Daemon daemon;
	private static volatile boolean exit = false;

	public static synchronized void launchDaemon(Context context) {
		long threshold = context.activeMemPercent();
		if (threshold < 100 && daemon == null) {
			daemon = new Daemon(threshold, ClusterParser.parser(context.url()));
			daemon.setDaemon(true);
			daemon.setName("ActiveMemPercentChecker");
			daemon.start();
			log.info("launch ActiveMemPercentChecker(daemon thread) OK");
			log.info(String.format("Can tune ActiveMemPercentChecker threshold %s by active_mem_percent config",threshold));
		}
	}

	public static void cancelDaemon() {
		exit = true;
		if (daemon != null)
			log.info("cancel ActiveMemPercentChecker(daemon thread) OK");
	}

	private static class Daemon extends Thread {
		private final long threshold;
		private List<Cluster> clusters;

		private Daemon(long threshold, List<Cluster> clusters) {
			this.clusters = clusters;
			this.threshold = threshold;
		}

		@Override
		public void run() {
			while (!exit) {
				try {
					Cluster master = FindMasterHandler.findMaster(this.clusters);
					long total = FetchValueHandler.fetchValue(master, FetchValueHandler.totalSQL);
					long limit = FetchValueHandler.fetchValue(master, FetchValueHandler.limitSQL);
					long percent = total * 100 / limit;
					Context.permit = percent < threshold;
					log.info(String
							.format("current OB updateserver memory table use [%s%%] {use[%s]/limit[%s] bytes}, writer config threshold[%s%%]. %s",
									percent,total,limit,threshold,Context.permit? "ok" : "suspend and wait target OB server auto release memory"));
					Thread.sleep(Context.daemon_check_interval);
				} catch (Throwable e) {
					log.warn("Daemon query OB error this time", e);
				}
			}
		}

		private static class FindMasterHandler implements ResultSetHandler<Cluster>{
			private static final FindMasterHandler instance = new FindMasterHandler();
			private static final String sql = "select * from __all_cluster where cluster_role = 1";
			
			private static Cluster findMaster(List<Cluster> clusters) throws Exception{
				for(Cluster cluster : clusters){
					try {
						return OBDataSource.executeJDBCQuery(cluster.ip, cluster.port, sql, instance);
					} catch (Exception e) {
						log.warn("can't find master cluster address",e);
					}
				}
				throw new IllegalStateException("can't find master cluster address");
			}
			
			@Override
			public Cluster callback(ResultSet result) throws Exception {
				if (result.next()){
					String ip = result.getString("cluster_vip");
					String port = result.getString("cluster_port");
					return new Cluster(ip,port);
				}
				throw new IllegalStateException("can't find master cluster address : return empty resultset");
			}
		}
		
		private static class FetchValueHandler implements ResultSetHandler<Long> {
			private static final FetchValueHandler instance = new FetchValueHandler();
			private static final String limitSQL;
			private static final String totalSQL;
			static {
				String template = "select stat.value as value from __all_server as server,__all_cluster as c, (select * from __all_server_stat) as stat where server.svr_role = 1 and c.cluster_role = 1 and c.cluster_id = server.cluster_id and server.svr_ip = stat.svr_ip and server.svr_type = 'updateserver' and stat.name like '%s'";
				limitSQL = String.format(template, "memory_limit");
				totalSQL = String.format(template, "memory_total");
			}
			
			public static long fetchValue(Cluster master, String sql) throws Exception{
				return OBDataSource.executeJDBCQuery(master.ip, master.port, sql, instance);
			}
			
			@Override
			public Long callback(ResultSet result) throws Exception {
				if (result.next()) {
					return result.getLong("value");
				}
				throw new IllegalStateException("Query total/limit return empty resultset");
			}
		}
	}

	private static class ClusterParser {
		private static Splitter clusterSplitter = Splitter.on(',')
				.trimResults().omitEmptyStrings();

		public static List<Cluster> parser(String configUrl) {
			try {
				Properties properties = new Properties();
				properties.load(new URL(configUrl).openStream());
				String clusterAddress = properties
						.getProperty("clusterAddress");
				if (clusterAddress == null) {
					throw new IllegalArgumentException(
							"config-url not contain clusterAddress property");
				}
				List<Cluster> clusters = Lists.newArrayList();
				for (String cluster : clusterSplitter.split(clusterAddress)) {
					clusters.add(new Cluster(cluster));
				}
				return clusters;
			} catch (Exception e) {
				throw new IllegalArgumentException("parse config-url error", e);
			}
		}
	}

	private static class Cluster {
		private final String ip;
		private final String port;
		private static Splitter hostPortSplitter = Splitter.on(':')
				.trimResults().omitEmptyStrings();

		private Cluster(String ip, String port) {
			this.ip = ip;
			this.port = port;
		}

		private Cluster(String cluster) {
			List<String> hostPort = Lists.newArrayList(hostPortSplitter
					.split(cluster));
			Preconditions.checkArgument(hostPort.size() == 2,
					"config-url clusterAddress property not correct [%s]",
					cluster);
			this.ip = hostPort.get(0);
			this.port = hostPort.get(1);
		}
	}
}