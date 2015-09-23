package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.core.transport.record.DefaultRecord;

import java.io.UnsupportedEncodingException;

public class RecordProducer {
	public static Record produceRecord() {

		try {
			Record record = new DefaultRecord();
			record.addColumn(ColumnProducer.produceLongColumn(1));
			record.addColumn(ColumnProducer.produceStringColumn("tair_writer"));
			record.addColumn(ColumnProducer.produceBoolColumn(true));
			record.addColumn(ColumnProducer.produceDateColumn(System
					.currentTimeMillis()));
			record.addColumn(ColumnProducer.produceBytesColumn("tair_writer"
					.getBytes("utf-8")));
			return record;
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException(e);
		}
	}

	public static Record produceRecord1() {

			Record record = new DefaultRecord();
			record.addColumn(ColumnProducer.produceLongColumn(1));
			return record;
	}
}
