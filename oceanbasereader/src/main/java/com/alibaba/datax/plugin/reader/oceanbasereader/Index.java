package com.alibaba.datax.plugin.reader.oceanbasereader;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Index implements Iterable<Index.Entry> {

	private Set<Entry> fields = new TreeSet<Entry>();

	private Index(Set<Entry> fields) {
		this.fields = fields;
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

		public enum Type {

			INT, VARCHAR {
				@Override
				public String convert(ResultSet result, String column)
						throws SQLException {
					return "'" + super.convert(result, column) + "'";
				}
			},
			TIMESTAMP {
				@Override
				public String convert(ResultSet result, String column)
						throws SQLException {
					return "timestamp'" + super.convert(result, column) + "'";
				}
			},
			NUMBER, UNKNOW, BOOL {
				@Override
				public String convert(ResultSet result, String column)
						throws SQLException {
					return String.valueOf(result.getBoolean(column));
				}
			};

			public String convert(ResultSet result, String column)
					throws SQLException {
				return result.getString(column);
			}

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

		private Builder() {
		};

		private boolean hasBuild = false;
		private Set<Entry> fields = new TreeSet<Entry>();

		public Builder addEntry(String name, String type, int position) {
			Preconditions.checkState(!hasBuild,"can't call addEntry() once build() has called");
			fields.add(new Entry(name, Entry.Type.type(type), position));
			return this;
		}

		public Index build() {
			Preconditions.checkState(!fields.isEmpty(),"first call addEntry() at least once");
			return new Index(this.fields);
		}
	}
}