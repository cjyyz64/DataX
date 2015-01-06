package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class RowkeyMeta implements Serializable,Iterable<RowkeyMeta.Entry> {

	private Set<Entry> fields = new TreeSet<Entry>();

	private RowkeyMeta(Set<Entry> fields) {
		this.fields = fields;
	}

	public static class Entry implements Serializable,Comparable<Entry> {

		public final String name;
		public final Type type;
        public final String id;
		public final int position;

		private Entry(String name, Type type, String id,int position) {
			this.name = name;
			this.type = type;
            this.id = id;
			this.position = position;
		}

        public String convert(String value) {
            return type.convert(value);
        }

		private enum Type {

			INT, VARCHAR {
				@Override
				public String convert(String value) {
					return "'" + value + "'";
				}
			},
			TIMESTAMP {
				@Override
				public String convert(String value) {
					return "timestamp'" + value + "'";
				}
			},
			NUMERIC, UNKNOW, BOOL;

            public String convert(String value) {
                return value;
            }

			public static Type type(String label) {
				label = label.toUpperCase();
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

		private Builder() { }

		private boolean hasBuild = false;
		private Set<Entry> fields = new TreeSet<Entry>();

		public Builder addEntry(String name, String type, String id, int position) {
			Preconditions.checkState(!hasBuild,"can't call addEntry() once build() has called");
			fields.add(new Entry(name, Entry.Type.type(type), id, position));
			return this;
		}

		public RowkeyMeta build() {
			Preconditions.checkState(!fields.isEmpty(),"first call addEntry() at least once");
			return new RowkeyMeta(this.fields);
		}
	}
}