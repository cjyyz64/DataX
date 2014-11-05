package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14-9-2.
 */
public class FakeWriter extends Writer {
	public static final class Master extends Writer.Master {

		@Override
		public List<Configuration> split(int readerSlicesNumber) {
			Configuration jobParameter = this.getPluginJobConf();
			System.out.println(jobParameter);

			List<Configuration> splitConfigurationList = new ArrayList<Configuration>();
			for (int i = 0; i < 1024; i++) {
				Configuration oneConfig = Configuration.newDefault();
				List<String> jdbcUrlArray = new ArrayList<String>();
				jdbcUrlArray.add(String.format("odps://localhost:3305/db%04d",
						i));
				oneConfig.set("odpsUrl", jdbcUrlArray);

				List<String> tableArray = new ArrayList<String>();
				tableArray.add(String.format("odps_jingxing_%04d", i));
				oneConfig.set("table", tableArray);

				splitConfigurationList.add(oneConfig);
			}

			return splitConfigurationList;
		}

		@Override
		public void init() {
			System.out.println("fake writer master initialized!");
		}

		@Override
		public void destroy() {
			System.out.println("fake writer master destroyed!");
		}
	}

	public static final class Slave extends Writer.Slave {

		@Override
		public void startWrite(RecordReceiver lineReceiver) {
			Record record = null;

			while ((record = lineReceiver.getFromReader()) != null) {
				this.getSlavePluginCollector().collectDirtyRecord(
						record,
						DataXException.asDataXException(FrameworkErrorCode.INNER_ERROR,
								"TEST"), "TEST");
			}

			for (int i = 0; i < 10; i++) {
				this.getSlavePluginCollector().collectMessage("bazhen-writer",
						"bazhen");
			}
		}

		@Override
		public void prepare() {
			System.out.println("fake writer slave prepared!");
		}

		@Override
		public void post() {
			System.out.println("fake writer slave posted!");
		}

		@Override
		public void init() {
			System.out.println("fake writer slave initialized!");
		}

		@Override
		public void destroy() {
			System.out.println("fake writer slave destroyed!");
		}
	}
}
