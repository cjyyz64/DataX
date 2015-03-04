package com.alibaba.datax.plugin.writer.oceanbasewriter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Rowkey implements Iterable<Rowkey.Entry> {

    private Set<Entry> fields = new TreeSet<Entry>();

    private Rowkey(Set<Entry> fields) {
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

            INT {
                @Override
                public Long value(ResultSet result, String column) throws SQLException {
                    return result.getLong(column);
                }

                @Override
                public void convert(int index, PreparedStatement statement, ResultSet result, String column) throws SQLException {
                    statement.setLong(index, result.getLong(column));
                }
            },
            VARCHAR {
                @Override
                public String value(ResultSet result, String column) throws SQLException {
                    return result.getString(column);
                }

                @Override
                public void convert(int index, PreparedStatement statement, ResultSet result, String column)
                        throws SQLException {
                    statement.setString(index, result.getString(column));
                }
            },
            TIMESTAMP {
                @Override
                public Timestamp value(ResultSet result, String column) throws SQLException {
                    return result.getTimestamp(column);
                }

                @Override
                public void convert(int index, PreparedStatement statement, ResultSet result, String column)
                        throws SQLException {
                    statement.setTimestamp(index, result.getTimestamp(column));
                }
            },
            NUMERIC {
                @Override
                public BigDecimal value(ResultSet result, String column) throws SQLException {
                    return result.getBigDecimal(column);
                }

                @Override
                public void convert(int index, PreparedStatement statement, ResultSet result, String column) throws SQLException {
                    statement.setBigDecimal(index, result.getBigDecimal(column));
                }
            },
            BOOL {
                @Override
                public Boolean value(ResultSet result, String column) throws SQLException {
                    return result.getBoolean(column);
                }

                @Override
                public void convert(int index, PreparedStatement statement, ResultSet result, String column)
                        throws SQLException {
                    statement.setBoolean(index, result.getBoolean(column));
                }
            };

            public abstract <T> T value(ResultSet result, String column) throws SQLException;
            public abstract void convert(int index, PreparedStatement statement, ResultSet result, String column) throws SQLException;

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
                throw new IllegalStateException("OB return a plugin not support type " + label);
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
        }

        ;

        private boolean hasBuild = false;
        private Set<Entry> fields = new TreeSet<Entry>();

        public Builder addEntry(String name, String type, int position) {
            Preconditions.checkState(!hasBuild, "can't call addEntry() once build() has called");
            fields.add(new Entry(name, Entry.Type.type(type), position));
            return this;
        }

        public Rowkey build() {
            Preconditions.checkState(!fields.isEmpty(), "first call addEntry() at least once");
            return new Rowkey(this.fields);
        }
    }
}