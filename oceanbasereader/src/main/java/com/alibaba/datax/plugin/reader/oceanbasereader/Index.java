package com.alibaba.datax.plugin.reader.oceanbasereader;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Index implements Iterable<Index.Entry> {

	private Set<Entry> fields = new TreeSet<Entry>();

	private Index(Set<Entry> fields) {
		this.fields = fields;
	}

    public static enum Type {

        INT {
            @Override
            public Long convert(ResultSet result, String column) throws SQLException {
                return result.getObject(column) == null? null : result.getLong(column);
            }
        }, VARCHAR {
            @Override
            public String convert(ResultSet result, String column)
                    throws SQLException {
                return result.getString(column);
            }
        }, TIMESTAMP {
            @Override
            public Timestamp convert(ResultSet result, String column)
                    throws SQLException {
                return result.getTimestamp(column);
            }
        },
        NUMERIC {
            @Override
            public BigDecimal convert(ResultSet result, String column) throws SQLException {
                return result.getBigDecimal(column);
            }
        }, UNKNOW {
            @Override
            public <T> T convert(ResultSet result, String column) throws SQLException {
                throw new IllegalArgumentException("not support field type for " + column);
            }
        }, BOOL {
            @Override
            public Boolean convert(ResultSet result, String column)
                    throws SQLException {
                return result.getObject(column) == null ? null : result.getBoolean(column);
            }
        };

        public abstract <T> T convert(ResultSet result, String column) throws SQLException;

        private static Map<String, String> DATETIME = ImmutableMap.of(
                "CREATETIME", "TIMESTAMP", "MODIFYTIME", "TIMESTAMP");

        public static Type type(String label) {
            label = label.toUpperCase();
            if (DATETIME.containsKey(label))
                label = DATETIME.get(label);
            for (Type type : Type.values()) {
                if (label.toUpperCase().contains(type.name()))
                    return type;
            }
            return UNKNOW;
        }
    }
	public static class Entry implements Comparable<Entry> {

		public final String name;
		public final Type type;
		public final int position;

		private Entry(String name, Type type, int position) {
			this.name = name;
			this.type = type;
			this.position = position;
		}


		@Override
		public int compareTo(Entry o) {
			return this.position - o.position;
		}
	}

	@Override
	public Iterator<Entry> iterator() {
		return fields.iterator();
	}

	public static Builder builder() {
		return new Builder();
	}

	public static final class Builder {

		private Builder() { }

		private boolean hasBuild = false;
		private Set<Entry> fields = new TreeSet<Entry>();

		public Builder addEntry(String name, String type, int position) {
			Preconditions.checkState(!hasBuild,"can't call addEntry() once build() has called");
			fields.add(new Entry(name, Type.type(type), position));
			return this;
		}

		public Index build() {
			Preconditions.checkState(!fields.isEmpty(),"first call addEntry() at least once");
			return new Index(this.fields);
		}
	}
}