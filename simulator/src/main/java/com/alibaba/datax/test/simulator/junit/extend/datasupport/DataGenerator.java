package com.alibaba.datax.test.simulator.junit.extend.datasupport;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class DataGenerator {
	private static List<String> CACHE_DATA = new ArrayList<String>();;

	// TODO
	public static void initCache(String[] absoluteTextFilePaths,
			String absolutePath) {
		for (String textFile : absoluteTextFilePaths) {
			try {
				CACHE_DATA.add(FileUtils.readFileToString(new File(textFile)));
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public static List<String> getCacheData() {
		return CACHE_DATA;
	}

}
