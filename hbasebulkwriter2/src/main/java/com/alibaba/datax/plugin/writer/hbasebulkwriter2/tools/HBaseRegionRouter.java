package com.alibaba.datax.plugin.writer.hbasebulkwriter2.tools;

import com.alibaba.datax.plugin.writer.hbasebulkwriter2.HBaseHelper;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.column.HBaseColumn;
import com.alibaba.datax.plugin.writer.hbasebulkwriter2.util.PhoenixEncoder;
import com.aliyun.odps.cache.DistributedCache;
import com.aliyun.odps.udf.UDF;
import org.apache.commons.cli.*;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.*;

public class HBaseRegionRouter extends UDF {
    private static final Logger LOG = LoggerFactory
            .getLogger(HBaseRegionRouter.class);
    private static final String FIELD_START = "start";
    private static final String FIELD_END = "end";
    private static final String ARG_TABLE = "t";
    private static final String ARG_CONF_PATH = "c";
    private static final String ARG_CLUSTER_ID = "l";
    private static final String ARG_CONFIGURATION = "n";
    private static final String ARG_REGIONS_FILE = "o";
    //private static final String PREFIX_PHOENIX_TYPE = "ph_";
    public static byte[][] keysArr = null;

    public static JSONArray getRegionsJson(String table, String confPath, String configStr, String clusterId) {
        Configuration conf = HBaseHelper.getConfiguration(null, confPath, configStr, clusterId);
        JSONArray regionsJson = new JSONArray();
        HTable htable = null;
        try {
            htable = new HTable(conf, Bytes.toBytes(table));

            Pair<byte[][], byte[][]> startEndKeysPair = htable
                    .getStartEndKeys();
            byte[][] startKeys = startEndKeysPair.getFirst();
            byte[][] endKeys = startEndKeysPair.getSecond();
            for (int i = 0, l = startKeys.length; i < l; i++) {
                JSONObject regionJson = new JSONObject();
                regionJson.put(FIELD_START, Hex.encodeHexString(startKeys[i]));
                regionJson.put(FIELD_END, Hex.encodeHexString(endKeys[i]));
                regionsJson.put(regionJson);
            }

        } catch (TableNotFoundException e) {
            LOG.error(String.format("Table %s isn't exist!", table), e);
            return null;
        } catch (IOException e) {
            LOG.error("Construct htable failed.", e);
            return null;
        } catch (JSONException e) {
            LOG.error("json parse exception", e);
            return null;
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    LOG.warn("fail to close htable ", e);
                }
            }
        }
        return regionsJson;
    }

    /**
     * Disable Logger when print the RegionsJson to console, because the Python
     * script read the RegionsJson from console.
     */
    public static void main(String[] args) {
        Options options = new Options();
        options.addOption(ARG_TABLE, true, "HBase table name");
        options.addOption(ARG_CONF_PATH, true, "HBase configure file path");
        options.addOption(ARG_CONFIGURATION, true, "HBase Configuration json(map.class)");
        options.addOption(ARG_CLUSTER_ID, true, "HBase cluster id");
        options.addOption(ARG_REGIONS_FILE, true, "HBase regions information output");
        CommandLineParser parser = new PosixParser();
        try {
            CommandLine line = parser.parse(options, args);

            Validate.isTrue(line.hasOption(ARG_TABLE),
                    "HBase table name couldn't be null.");
            final String table = line.getOptionValue(ARG_TABLE);

            final String confPath = line.getOptionValue(ARG_CONF_PATH);
            final String clusterId = line.getOptionValue(ARG_CLUSTER_ID);
            final String configStr = line.getOptionValue(ARG_CONFIGURATION);
            final String regionsFile = line.getOptionValue(ARG_REGIONS_FILE);

            LOG.info(String.format("HBaseRegionRouter run confPath=%s,clusterId=%s,configStr=%s,regionsFile=%s", confPath, clusterId, configStr, regionsFile));
            if (StringUtils.isEmpty(regionsFile)) {
                HBaseHelper.disableLogger();
            }

            ExecutorService executor = Executors.newSingleThreadExecutor();
            Future<JSONArray> future = executor.submit(new Callable<JSONArray>() {
                @Override
                public JSONArray call() {
                    return getRegionsJson(table, confPath, configStr, clusterId);
                }
            });
            //JSONArray regionsJson = future.get(30, TimeUnit.SECONDS);
            JSONArray regionsJson = future.get();

            if (StringUtils.isEmpty(regionsFile)) {
                System.out.println(regionsJson);
                HBaseHelper.enableLogger();
            } else {
                FileUtils.writeStringToFile(new File(regionsFile), regionsJson.toString());
            }
        } catch (Exception e) {
            LOG.error("Get regionsJson failed, check your parameters and make sure you could connect HBase successfully in your environment.", e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("HBaseRegionRouterHelper", options);
            System.exit(-1);
        }

        System.exit(0);
    }

    public byte[][] toKeysArr(JSONArray regionsJson) throws JSONException {
        int regionsCount = regionsJson.length();
        byte[][] keysArr = new byte[regionsCount][];
        for (int i = 0; i < regionsCount; i++) {
            JSONObject regionJson = regionsJson.getJSONObject(i);
            try {
                keysArr[i] = Hex.decodeHex(regionJson.getString(FIELD_START)
                        .toCharArray());
            } catch (DecoderException e) {
                LOG.error("Parse Error", e);
            }
        }
        return keysArr;
    }

    public int findRegionNum(byte[][] keysArr, byte[] rowkey) {
        int index = Arrays
                .binarySearch(keysArr, rowkey, Bytes.BYTES_COMPARATOR);
        if (index < 0) {
            index = -(index + 1) - 1;
        }
        return index;
    }

    /**
     * Find rowkey position in region ranges
     *
     * @param args
     * @return
     */
    public String findRegionNumByRowkeyHex(String... args) {
        if (args.length != 2) {
            throw new RuntimeException("Choice: 2, Check the parameters of udf function!");
        }
        String resource = args[0];
        String rowkeyHex = args[1];

        if (keysArr == null) {
            String regionsStr = null;
            try {
                regionsStr = IOUtils.toString(DistributedCache
                        .readCacheFileAsStream(resource));
            } catch (IOException e) {
                throw new RuntimeException("Get resource failed.", e);
            }

            JSONArray regionsJson;
            try {
                regionsJson = new JSONArray(regionsStr);
                keysArr = toKeysArr(regionsJson);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }

        }

        try {
            byte[] rowkey = Hex.decodeHex(rowkeyHex.toCharArray());
            int index = findRegionNum(keysArr, rowkey);
            return index + "";
        } catch (DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Convert args to rowkey.
     * When the number of args less than 2, it would return a empty byte array.
     *
     * @param args
     * @return
     */
    public byte[] toRowkey(String... args) {
        int len = args.length / 2;
        byte[] rowkey = {};
        for (int i = 0; i < len; i++) {
            HBaseColumn.HBaseDataType type = HBaseColumn.HBaseDataType.parseStr(args[2 * i + 1]);
            byte[] tmp = HBaseColumn.convertStrToBytes(args[2 * i], type);
            rowkey = Bytes.add(rowkey, tmp);
        }
        return rowkey;
    }

    /**
     * Convert args to Phoenix style rowkey.
     * When the number of args less than 2, it would return a empty byte array.
     *
     * @param args
     * @return
     */
    public byte[] toPhoenixStyleRowkey(String... args) {
        byte[] rowkey = {};
        if (args.length < 3) {
            return rowkey;
        }

        int bucketNum = Integer.parseInt(args[0]);
        String[] newArgs = new String[args.length - 1];
        System.arraycopy(args, 1, newArgs, 0, newArgs.length);
        args = newArgs;

        int len = args.length / 2;
        for (int i = 0; i < len; i++) {
            HBaseColumn.HBaseDataType type = HBaseColumn.HBaseDataType.parseStr(args[2 * i + 1], true);
            byte[] tmp = HBaseColumn.convertStrToBytes(args[2 * i], type);
            rowkey = Bytes.add(rowkey, tmp);
        }
        if (bucketNum != -1) {
            rowkey = PhoenixEncoder.getSaltedKey(rowkey, bucketNum);
        }
        return rowkey;
    }

    /**
     * Key structure:
     * |rowkey|column|timestamp|
     *
     * @param args
     * @return key bytes
     */
    public byte[] toKeyValueKey(String... args) {
        byte[] key = {};
        if (args.length != 6) {
            return key;
        }

        HBaseColumn.HBaseDataType type = HBaseColumn.HBaseDataType.parseStr(args[1]);
        byte[] rowkey = HBaseColumn.convertStrToBytes(args[0], type);
        key = Bytes.add(key, rowkey);
        type = HBaseColumn.HBaseDataType.parseStr(args[3]);
        byte[] column = HBaseColumn.convertStrToBytes(args[2], type);
        key = Bytes.add(key, column);
        type = HBaseColumn.HBaseDataType.parseStr(args[5]);
        assert (type.equals(HBaseColumn.HBaseDataType.LONG));
        byte[] timestamp = Bytes.toBytes(Long.MAX_VALUE - Long.parseLong(args[4]));
        key = Bytes.add(key, timestamp);

        return key;
    }

    /**
     * This is the entrance of udf.
     * When choice is 1, it would return the hex string of rowkey.
     * And when choice is 2, it would return the position number of region.
     *
     * @param choice
     * @param args
     * @return
     */
    public String evaluate(Long choice, String... args) {
        if (choice == 1) {
            // Convert args to rowkey.
            return Hex.encodeHexString(toRowkey(args));
        } else if (choice == 2) {
            // Find the position number of region by given rowkeyHex.
            return findRegionNumByRowkeyHex(args);
        } else if (choice == 3) {
            // Convert args to Phoenix style rowkey.
            return Hex.encodeHexString(toPhoenixStyleRowkey(args));
        } else if (choice == 4) {
            // Convert args to KeyValue#getKey()
            return Hex.encodeHexString(toKeyValueKey(args));
        } else {
            throw new RuntimeException(String.format("Unsupported choice: %s.", choice));
        }
    }
}
