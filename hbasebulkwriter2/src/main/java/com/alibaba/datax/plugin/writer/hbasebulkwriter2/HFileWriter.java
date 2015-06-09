package com.alibaba.datax.plugin.writer.hbasebulkwriter2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.*;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;

/**
 * Writes HFiles. Passed KeyValues must arrive in order. Currently, can only
 * write files to a single column family at a time. Multiple column families
 * requires coordinating keys cross family. Writes current time as the sequence
 * id for the file. Sets the major compacted attribute on created hfiles.
 * Calling write(null,null) will forceably roll all HFiles being written.
 * 
 * This class comes from @see
 * org.apache.hadoop.hbase.mapreduce.HFileOutputFormat#getRecordWriter directly.
 */
public class HFileWriter {

	static final String COMPRESSION_CONF_KEY = "hbase.hfileoutputformat.families.compression";
	static final String COMPRESSION_BASE_ON_CFNAME = "hbase.hfileoutputformat.compression.baseon.families";
	private static final String DATABLOCK_ENCODING_CONF_KEY = "hbase.mapreduce.hfileoutputformat.datablock.encoding";
  private static final String BLOOM_TYPE_CONF_KEY = "hbase.hfileoutputformat.families.bloomtype";
	static Log LOG = LogFactory.getLog(HFileWriter.class);
	// create a map from column family to the compression algorithm
	final Map<byte[], String> compressionMap;
  final Map<byte[], String> bloomTypeMap;
	final HFileDataBlockEncoder encoder;
	// Map of families to writers and how much has been output on the writer.
	private final Map<byte[], WriterLength> writers = new TreeMap<byte[], WriterLength>(
			Bytes.BYTES_COMPARATOR);
	private final byte[] now = Bytes.toBytes(System.currentTimeMillis());
  // Get the path of the temporary output file
	Path outputdir;
	Configuration conf;
	FileSystem fs;
	// These configs. are from hbase-*.xml
	long maxsize;
	int blocksize;
	// Invented config. Add to hbase-*.xml if other than default compression.
	String defaultCompression;
	boolean turnOffCompression;
	boolean compactionExclude;
	String dataBlockEncodingStr;
	private byte[] previousRow = HConstants.EMPTY_BYTE_ARRAY;
	private boolean rollRequested = false;

	public HFileWriter(String outputdirStr, Configuration conf)
			throws IOException {
		outputdir = new Path(outputdirStr);
		this.conf = conf;
		fs = outputdir.getFileSystem(conf);
		maxsize = conf.getLong(HConstants.HREGION_MAX_FILESIZE,
				HConstants.DEFAULT_MAX_FILE_SIZE);
		blocksize = conf.getInt("hbase.mapreduce.hfileoutputformat.blocksize",
				HFile.DEFAULT_BLOCKSIZE);
		defaultCompression = conf.get("hfile.compression",
				Compression.Algorithm.NONE.getName());
		turnOffCompression = conf
				.getBoolean("hfile.compression.turnoff", false);
		compactionExclude = conf.getBoolean(
				"hbase.mapreduce.hfileoutputformat.compaction.exclude", false);
		dataBlockEncodingStr = conf.get(DATABLOCK_ENCODING_CONF_KEY);
		compressionMap = createFamilyCompressionMap(conf);
    bloomTypeMap = createFamilyBloomMap(conf);

		if (dataBlockEncodingStr == null) {
			encoder = NoOpDataBlockEncoder.INSTANCE;
		} else {
			try {
				encoder = new HFileDataBlockEncoderImpl(
						DataBlockEncoding.valueOf(dataBlockEncodingStr));
			} catch (IllegalArgumentException ex) {
				throw new RuntimeException(
						"Invalid data block encoding type configured for the param "
								+ DATABLOCK_ENCODING_CONF_KEY + " : "
								+ dataBlockEncodingStr);
			}
		}
	}

	/**
	 * Run inside the task to deserialize column family to compression algorithm
	 * map from the configuration.
	 *
	 * Package-private for unit tests only.
	 *
	 * @return a map from column family to the name of the configured
	 *         compression algorithm
	 */
	static Map<byte[], String> createFamilyCompressionMap(Configuration conf) {
		//Map<byte[], String> compressionMap = new TreeMap<byte[], String>(
				//Bytes.BYTES_COMPARATOR);
    return createFamilyConfValueMap(conf, COMPRESSION_CONF_KEY);
  }

  private static Map<byte[], String> createFamilyBloomMap(Configuration conf) {
    return createFamilyConfValueMap(conf, BLOOM_TYPE_CONF_KEY);
  }

  /**
   * Run inside the task to deserialize column family to given conf value map.
   *
   * @param conf
   * @param confName
   * @return a map of column family to the given configuration value
   */
  private static Map<byte[], String> createFamilyConfValueMap(Configuration conf, String confName) {
    Map<byte[], String> confValMap = new TreeMap<byte[], String>(Bytes.BYTES_COMPARATOR);
    String confVal = conf.get(confName, "");
    for (String familyConf : confVal.split("&")) {
			String[] familySplit = familyConf.split("=");
			if (familySplit.length != 2) {
				continue;
			}

			try {
        confValMap.put(URLDecoder.decode(familySplit[0], "UTF-8").getBytes(),
                URLDecoder.decode(familySplit[1], "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				// will not happen with UTF-8 encoding
				throw new AssertionError(e);
			}
		}
    return confValMap;
	}

  /**
   * Serialize column family to compression algorithm map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * Package-private for unit tests only.
   *
   * @throws java.io.IOException
   *           on failure to read column family descriptors
   */
  static void configureCompression(HTable table, Configuration conf) throws IOException {
    StringBuilder compressionConfigValue = new StringBuilder();
    String tableName = Bytes.toString(table.getTableName());
    @SuppressWarnings("deprecation")
    HTableDescriptor tableDescriptor = table.getConnection().getHTableDescriptors(Collections.singletonList(tableName))[0];
    if(tableDescriptor == null){
      // could happen with mock table instance
      return;
    }
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        compressionConfigValue.append('&');
      }
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
      compressionConfigValue.append('=');
      compressionConfigValue.append(URLEncoder.encode(familyDescriptor.getCompression().getName(), "UTF-8"));
    }
    // Get rid of the last ampersand
    conf.set(COMPRESSION_CONF_KEY, compressionConfigValue.toString());
  }

  /**
   * Serialize column family to bloom type map to configuration.
   * Invoked while configuring the MR job for incremental load.
   *
   * @throws java.io.IOException
   *           on failure to read column family descriptors
   */
  static void configureBloomType(HTable table, Configuration conf) throws IOException {
	String tableName = Bytes.toString(table.getTableName());
	@SuppressWarnings("deprecation")
  HTableDescriptor tableDescriptor = table.getConnection().getHTableDescriptors(Collections.singletonList(tableName))[0];
    if (tableDescriptor == null) {
      // could happen with mock table instance
      return;
    }
    StringBuilder bloomTypeConfigValue = new StringBuilder();
    Collection<HColumnDescriptor> families = tableDescriptor.getFamilies();
    int i = 0;
    for (HColumnDescriptor familyDescriptor : families) {
      if (i++ > 0) {
        bloomTypeConfigValue.append('&');
      }
      bloomTypeConfigValue.append(URLEncoder.encode(familyDescriptor.getNameAsString(), "UTF-8"));
      bloomTypeConfigValue.append('=');
      String bloomType = familyDescriptor.getBloomFilterType().toString();
      if (bloomType == null) {
        bloomType = HColumnDescriptor.DEFAULT_BLOOMFILTER;
      }
      bloomTypeConfigValue.append(URLEncoder.encode(bloomType, "UTF-8"));
    }
    conf.set(BLOOM_TYPE_CONF_KEY, bloomTypeConfigValue.toString());
  }

	public void write(ImmutableBytesWritable row, KeyValue kv)
			throws IOException {
		// null input == user explicitly wants to flush
		if (row == null && kv == null) {
			return;
		}

		byte[] rowKey = kv.getRow();
		long length = kv.getLength();
		byte[] family = kv.getFamily();
		WriterLength wl = this.writers.get(family);

		// If this is a new column family, verify that the directory exists
		if (wl == null) {
			fs.mkdirs(new Path(outputdir, Bytes.toString(family)));
		}

		// If any of the HFiles for the column families has reached
		// maxsize, we need to roll all the writers
		if (wl != null && wl.written + length >= maxsize) {
			this.rollRequested = true;
		}

		// This can only happen once a row is finished though
		if (rollRequested && Bytes.compareTo(this.previousRow, rowKey) != 0) {
			rollWriters();
		}

		// create a new HLog writer, if necessary
		if (wl == null || wl.writer == null) {
			wl = getNewWriter(family, conf);
		}

		// we now have the proper HLog writer. full steam ahead
		kv.updateLatestStamp(this.now);
		wl.writer.append(kv);
		wl.written += length;

		// Copy the row so we know when a row transition.
		this.previousRow = rowKey;
	}

	private void rollWriters() throws IOException {
		for (WriterLength wl : this.writers.values()) {
			if (wl.writer != null) {
				LOG.info("Writer=" + wl.writer.getPath()
						+ ((wl.written == 0) ? "" : ", wrote=" + wl.written));
				close(wl.writer);
			}
			wl.writer = null;
			wl.written = 0;
		}
		this.rollRequested = false;
	}

	/*
	 * Create a new StoreFile.Writer.
	 *
	 * @param family
	 *
   * @return A WriterLength, containing a new StoreFile.Writer.
	 *
	 * @throws IOException
	 */
	private WriterLength getNewWriter(byte[] family, Configuration conf)
			throws IOException {
		WriterLength wl = new WriterLength();
		Path familydir = new Path(outputdir, Bytes.toString(family));
		String compression = defaultCompression;
    BloomType bloomType = BloomType.ROW;
    Configuration tempConf = new Configuration(conf);
    tempConf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0.0f);
    wl.writer = new StoreFile.WriterBuilder(conf, new CacheConfig(tempConf), fs, blocksize)
        .withOutputDir(familydir)
        .withCompression(AbstractHFileWriter.compressionByName(compression))
        .withBloomType(bloomType)
        .withComparator(KeyValue.COMPARATOR)
        .withDataBlockEncoder(encoder)
        .withChecksumType(Store.getChecksumType(conf))
        .withBytesPerChecksum(Store.getBytesPerChecksum(conf))
        .build();

		this.writers.put(family, wl);
		return wl;
	}

	private void close(final StoreFile.Writer w) throws IOException {
		if (w != null) {
			w.appendFileInfo(StoreFile.BULKLOAD_TIME_KEY,
					Bytes.toBytes(System.currentTimeMillis()));
			w.appendFileInfo(StoreFile.MAJOR_COMPACTION_KEY,
					Bytes.toBytes(true));
			w.appendFileInfo(StoreFile.EXCLUDE_FROM_MINOR_COMPACTION_KEY,
					Bytes.toBytes(compactionExclude));
      w.appendFileInfo(StoreFile.DATA_BLOCK_ENCODING, encoder
              .getEncodingOnDisk().getNameInBytes());
      w.appendTrackedTimestampsToMetadata();
			w.close();
		}
	}

	public void close() throws IOException,
			InterruptedException {
		for (WriterLength wl : this.writers.values()) {
			close(wl.writer);
		}
	}

	/*
	 * Data structure to hold a Writer and amount of data written on it.
	 */
	static class WriterLength {
		long written = 0;
    StoreFile.Writer writer = null;
	}
}
