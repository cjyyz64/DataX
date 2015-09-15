package com.alibaba.datax.plugin.reader.oceanbasereader.utils;

import com.google.common.base.Preconditions;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

public class RowkeyMeta implements Serializable,Iterable<RowkeyMeta.Entry> {

	private Set<Entry> fields = new TreeSet<Entry>();

	private RowkeyMeta(Set<Entry> fields) {
		this.fields = fields;
	}

    public static enum BoundValue{
        OB_MIN{
            @Override
            public String toString(){
                return "__OB__MIN__";
            }
        },OB_MAX{
            @Override
            public String toString(){
                return "__OB__MAX__";
            }
        };

        public static boolean isMin(Object value){
           return value != null && (value == OB_MIN || OB_MIN.toString().equalsIgnoreCase(value.toString()));
        }

        public static boolean isMax(Object value){
            return value!= null && (value == OB_MAX || OB_MAX.toString().equalsIgnoreCase(value.toString()));
        }
    }

    private static enum Type {

        INT {
            @Override
            public Long value(ResultSet result, String name) throws SQLException{
                return result.getLong(name);
            }
        }, VARCHAR {
            @Override
            public String value(ResultSet result, String name) throws SQLException {
                return result.getString(name);
            }
        },
        TIMESTAMP {
            @Override
            public String value(ResultSet result, String name) throws SQLException {
                Timestamp timestamp = result.getTimestamp(name);
                return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS").format(timestamp);
            }
        },
        NUMERIC {
            @Override
            public BigDecimal value(ResultSet result, String name) throws SQLException {
                return result.getBigDecimal(name);
            }
        }, UNKNOW {
            @Override
            public <T> T value(ResultSet result, String name) throws SQLException {
                throw new IllegalArgumentException("not support field type for " + name);
            }
        }, BOOL {
            @Override
            public Boolean value(ResultSet result, String name) throws SQLException {
                return result.getBoolean(name);
            }
        };

        public abstract <T> T value(ResultSet result,String name) throws SQLException;

        public static Type type(String label) {
            label = label.toUpperCase();
            for (Type type : Type.values()) {
                if (label.toUpperCase().contains(type.name()))
                    return type;
            }
            return UNKNOW;
        }
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

        public <T> T value(ResultSet result,String name) throws Exception{
            return type.value(result,name);
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
			fields.add(new Entry(name, Type.type(type), id, position));
			return this;
		}

		public RowkeyMeta build() {
			Preconditions.checkState(!fields.isEmpty(),"first call addEntry() at least once");
			return new RowkeyMeta(this.fields);
		}
	}
}