package com.alibaba.datax.core.statistics.metric;

import com.alibaba.fastjson.JSON;

import org.apache.commons.lang.Validate;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by jingxing on 14-9-15.
 */
public class MetricJsonify {
    private static Pair<String, Long> getTotalBytes(final Metric metric) {
        return new Pair<String, Long>("totalBytes", metric.getWriteSucceedByteNumber());
    }

    private static Pair<String, Long> getTotalRecords(final Metric metric) {
        return new Pair<String, Long>("totalRecords", metric.getWriteSucceedRecordNumber());
    }

    private static Pair<String, Long> getSpeedByte(final Metric metric) {
        return new Pair<String, Long>("speedBytes", metric.getByteSpeed());
    }

    private static Pair<String, Long> getSpeedRecord(final Metric metric) {
        return new Pair<String, Long>("speedRecords", metric.getRecordSpeed());
    }

    private static Pair<String, Long> getErrorRecords(final Metric metric) {
        return new Pair<String, Long>("errorRecords",
                metric.getTotalReadRecords() - metric.getWriteSucceedRecordNumber());
    }

    private static Pair<String, Double> getStage(final Metric metric) {
        return new Pair<String, Double>("stage", metric.getPercentage());
    }

    private static Pair<String, String> getErrorMessage(final Metric metric) {
        return new Pair<String, String>("errorMessage", metric.getException());
    }

    @SuppressWarnings("rawtypes")
	public static String getSnapshot(Metric metric) {
        Validate.notNull(metric);

        Map<String, Object> state = new HashMap<String, Object>();

		Pair pair = getTotalBytes(metric);
        state.put((String)pair.getKey(), pair.getValue());

        pair = getTotalRecords(metric);
        state.put((String)pair.getKey(), pair.getValue());

        pair = getSpeedRecord(metric);
        state.put((String)pair.getKey(), pair.getValue());

        pair = getSpeedByte(metric);
        state.put((String)pair.getKey(), pair.getValue());

        pair = getStage(metric);
        state.put((String)pair.getKey(), pair.getValue());

        pair = getErrorRecords(metric);
        state.put((String)pair.getKey(), pair.getValue());

        pair = getErrorMessage(metric);
        state.put((String)pair.getKey(), pair.getValue());

        return JSON.toJSONString(state);
    }

    static class Pair<K, V> {
        public Pair(final K key, final V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }

        private K key;

        private V value;
    }
}
