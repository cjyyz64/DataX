package com.alibaba.datax.plugin.writer.tairwriter;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.datax.common.util.Configuration;

public class TairConfig {

    private static final Logger LOG = LoggerFactory.getLogger(TairClientManager.class);
    private String error = "success";
    private Configuration conf;

    private String configId;
    private int namespace;
    private String language;
    private String writerType;//put, prefixput, multiprefixput / counter, prefixcounter, multiprefixcounter
    private List<String> skeyList = null;
    private char fieldDelimiter;
    private boolean deleteEmptyRecord;
    private int expire;
    private int compressionThreshold;
    private int timeout;

    TairConfig(Configuration f) {
        conf = f;
    }

    public boolean checkValid() {
        this.configId = conf.getString(Key.CONFIG_ID, null);
        this.namespace = conf.getInt(Key.NAMESPACE, -1);
        this.expire = conf.getInt(Key.EXPIRE, 0);
        this.timeout = conf.getInt(Key.TIMEOUT, 2000);
        this.writerType = conf.getString(Key.WRITER_TYPE, null);
        this.language = conf.getString(Key.LANGUAGE, null);

        this.compressionThreshold = conf.getInt(Key.COMPRESSION_THRESHOLD, -1);

        if (configId == null) {
          error = "configid 为空";
          return false;
        }

        if (namespace <= 0) {
          error = "无效的 namespace: " + namespace;
          return false;
        }

        if (timeout <= 0) {
          error = "无效的 timeout: " + timeout;
          return false;
        }

        if (language == null) {
            error = "language must config";
            return false;
        } else if (language.equalsIgnoreCase("java")) {
            if (compressionThreshold <= 0) {
              error = "没有配置或无效的 compressionThreshold: " + compressionThreshold;
              return false;
            }
        } else if (language.equalsIgnoreCase("c++")) {
            if (compressionThreshold > 0) {
              error = "compressionThreshold 只对 java 有效";
              return false;
            }
        } else {
          error = "无效的 language: " + language;
          return false;
        }

        if (!writerType.equalsIgnoreCase("put") &&
                !writerType.equalsIgnoreCase("counter") &&
                !writerType.equalsIgnoreCase("prefixput") &&
                !writerType.equalsIgnoreCase("prefixcounter") &&
                !writerType.equalsIgnoreCase("multiprefixput") &&
                !writerType.equalsIgnoreCase("multiprefixcounter")) {
            error = "未知的写入类型 writerType:" + writerType;
            return false;
        }
        LOG.info("TairWriter use {} Type.", writerType);

        String delimiter = conf.getString(Key.FIELD_DELIMITER, null);
        if (delimiter != null) {
            if (writerType.equalsIgnoreCase("put")) {
                this.fieldDelimiter = delimiter.charAt(0);
            } else {
                error = "fieldDelimiter 仅仅在 writeType 为 put 有效";
                return false;
            }
        } else if (writerType.equalsIgnoreCase("put")) {
            error = "fieldDelimiter 在 writeType 为 put 时必须配置";
            return false;
        }

        String deleteEmpty = conf.getString(Key.DELETE_EMPTY_RECORD, null);
        if (deleteEmpty != null) {
           if (writerType.equalsIgnoreCase("put")) {
              this.deleteEmptyRecord = deleteEmpty.equalsIgnoreCase("true");
           } else {
              error = "deleteEmptyRecord 只对 writeType 为 put 时有效";
              return false;
           }
        } else if (writerType.equalsIgnoreCase("put")) {
            error = "deleteEmptyRecord 在 writeType 为 put 时必须配置";
            return false;
        }

        this.skeyList = conf.getList(Key.SKEY_LIST, null, String.class);
        if (this.skeyList != null) {
            if (!writerType.equalsIgnoreCase("multiprefixput") &&
                    !writerType.equalsIgnoreCase("multiprefixcounter")) {
                error = "只有multiprefixput 或 multiprefixcounter 才需要指定 skeyList";
                return false;
            } else if (this.skeyList.size() == 0) {
                error = "multiprefixput 或 multiprefixcounter 至少指定一个 skey";
                return false;
            }
            for (String skey : this.skeyList) {
                if (skey.length() == 0) {
                    error = "exist skey is empty string, skeyList:" + this.skeyList;
                    return false;
                }
            }
        } else if (writerType.equalsIgnoreCase("multiprefixput") ||
                writerType.equalsIgnoreCase("multiprefixcounter")) {
            error = "multiprefixput 或 multiprefixcounter 必须指定 skeyList";
            return false;
        }
        return true;
    }

    public String getConfigId() {
        return configId;
    }

    public int getNamespace() {
        return namespace;
    }

    public String getLanguage() {
        return language;
    }

    public String getWriterType() {
        return writerType;
    }

    public char getFieldDelimiter() {
        return fieldDelimiter;
    }

    public boolean isDeleteEmptyRecord() {
        return deleteEmptyRecord;
    }

    public int getExpire() {
        return expire;
    }

    public int getCompressionThreshold() {
        return compressionThreshold;
    }

    public int getTimeout() {
        return timeout;
    }

    public List<String> getSkeyList() {
        return skeyList;
    }

    public String getErrorString() {
        return error;
    }
}
