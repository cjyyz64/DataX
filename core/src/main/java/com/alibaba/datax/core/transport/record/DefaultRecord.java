package com.alibaba.datax.core.transport.record;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;

/**
 * Created by jingxing on 14-8-24.
 */

public class DefaultRecord implements Record {

	private static final int RECORD_AVERGAE_COLUMN_NUMBER = 16;

	private List<Column> columns;

	private int byteSize;

	public DefaultRecord() {
		this.columns = new ArrayList<Column>(RECORD_AVERGAE_COLUMN_NUMBER);
	}

	@Override
	public void addColumn(Column column) {
		columns.add(column);
		incrByteSize(column);
	}

	@Override
	public Column getColumn(int i) {
		if (i < 0 || i >= columns.size()) {
			return null;
		}
		return columns.get(i);
	}

	@Override
	public void setColumn(int i, final Column column) {
		if (i < 0) {
			throw new DataXException(FrameworkErrorCode.ARGUMENT_ERROR,
					"Can not set column which index less than 0");
		}

		if (i >= columns.size()) {
			expandCapacity(i + 1);
		}

		decrByteSize(getColumn(i));
		this.columns.set(i, column);
		incrByteSize(getColumn(i));
	}

	@Override
	public String toString() {
		return StringUtils.join(columns, ",");
	}

	@Override
	public int getColumnNumber() {
		return this.columns.size();
	}

	@Override
	public int getByteSize() {
		return byteSize;
	}

	private void decrByteSize(final Column column) {
		if (null == column) {
			return;
		}

		byteSize -= column.getByteSize();
	}

	private void incrByteSize(final Column column) {
		if (null == column) {
			return;
		}

		byteSize += column.getByteSize();
	}

	private void expandCapacity(int totalSize) {
		if (totalSize <= 0) {
			return;
		}

		int needToExpand = totalSize - columns.size();
		while (needToExpand-- > 0) {
			this.columns.add(null);
		}
	}

}
