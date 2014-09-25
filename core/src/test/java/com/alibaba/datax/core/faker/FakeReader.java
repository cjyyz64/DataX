package com.alibaba.datax.core.faker;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.common.element.NumberColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.core.util.FrameworkErrorCode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jingxing on 14-9-2.
 */
public class FakeReader extends Reader {
	public static final class Master extends Reader.Master {
		@Override
		public List<Configuration> split(int adviceNumber) {
			Configuration jobParameter = this.getPluginJobConf();
			System.out.println(jobParameter);

			List<Configuration> splitConfigurationList = new ArrayList<Configuration>();
			for (int i = 0; i < 1024; i++) {
				Configuration oneConfig = Configuration.newDefault();
				List<String> jdbcUrlArray = new ArrayList<String>();
				jdbcUrlArray.add(String.format(
						"jdbc:mysql://localhost:3305/db%04d", i));
				oneConfig.set("jdbcUrl", jdbcUrlArray);

				List<String> tableArray = new ArrayList<String>();
				tableArray.add(String.format("jingxing_%04d", i));
				oneConfig.set("table", tableArray);

				splitConfigurationList.add(oneConfig);
			}

			return splitConfigurationList;
		}

		@Override
		public void init() {
			System.out.println("fake reader master initialized!");
		}

		@Override
		public void destroy() {
			System.out.println("fake reader master destroyed!");
		}
	}

	public static final class Slave extends Reader.Slave {
		@Override
		public void startRead(RecordSender lineSender) {
			Record record = lineSender.createRecord();
			record.addColumn(new NumberColumn(1L));

			for (int i = 0; i < 10; i++) {
				lineSender.sendToWriter(record);
			}

			for (int i = 0; i < 10; i++) {
				this.getSlavePluginCollector().collectDirtyRecord(
						record,
						new DataXException(FrameworkErrorCode.INNER_ERROR,
								"TEST"), "TEST");
			}

			for (int i = 0; i < 10; i++) {
				this.getSlavePluginCollector().collectDirtyRecord(record,
						"TEST");
			}

			for (int i = 0; i < 10; i++) {
				this.getSlavePluginCollector().collectDirtyRecord(
						record,
						new DataXException(FrameworkErrorCode.INNER_ERROR,
								"TEST"));
			}

			for (int i = 0; i < 10; i++) {
				this.getSlavePluginCollector().collectMessage("bazhen-reader",
						"bazhen");
			}
		}

		@Override
		public void prepare() {
			System.out.println("fake reader slave prepared!");
		}

		@Override
		public void post() {
			System.out.println("fake reader slave posted!");
		}

		@Override
		public void init() {
			System.out.println("fake reader slave initialized!");
		}

		@Override
		public void destroy() {
			System.out.println("fake reader master destroyed!");
		}
	}
}
