package com.alibaba.datax.plugin.writer.tairwriter;

import com.alibaba.datax.common.element.*;

public class ColumnProducer {
	public static Column produceLongColumn(int i) {
		return new LongColumn(i);
	}

	public static Column produceStringColumn(String s) {
		return new StringColumn(s);
	}

	public static Column produceDateColumn(long time) {
		return new DateColumn(time);
	}

	public static Column produceBytesColumn(byte[] bytes) {
		return new BytesColumn(bytes);
	}

	public static Column produceBoolColumn(boolean bool) {
		return new BoolColumn(bool);
	}
}
