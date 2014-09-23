package com.alibaba.datax.core.container.util;

import java.io.File;
import java.io.FileFilter;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;

/**
 * 提供Jar隔离的加载机制
 * 
 */
public class JarLoader extends URLClassLoader {
	public JarLoader(String[] paths) {
		this(paths, JarLoader.class.getClassLoader());
	}

	public JarLoader(String[] paths, ClassLoader parent) {
		super(getURLs(paths), parent);
	}

	private static URL[] getURLs(String[] paths) {
		Validate.isTrue(null != paths && 0 != paths.length,
				"Paths cannot be empty .");

		List<String> dirs = new ArrayList<String>();
		for (String path : paths) {
			dirs.add(path);
			JarLoader.collectDirs(path, dirs);
		}

		List<URL> urls = new ArrayList<URL>();
		for (String path : dirs) {
			urls.addAll(doGetURLs(path));
		}

		return urls.toArray(new URL[0]);
	}

	private static void collectDirs(String path, List<String> collector) {
		if (null == path || StringUtils.isBlank(path)) {
			return;
		}

		File current = new File(path);
		if (!current.exists() || !current.isDirectory()) {
			return;
		}

		for (File child : current.listFiles()) {
			if (!child.isDirectory()) {
				continue;
			}

			collector.add(child.getAbsolutePath());
			collectDirs(child.getAbsolutePath(), collector);
		}
	}

	private static List<URL> doGetURLs(final String path) {
		Validate.isTrue(!StringUtils.isBlank(path), "Path cannot be empty .");

		File jarPath = new File(path);

		Validate.isTrue(jarPath.exists() && jarPath.isDirectory(),
				"Path must exists and be directory .");

		/* set filter */
		FileFilter jarFilter = new FileFilter() {
			@Override
			public boolean accept(File pathname) {
				return pathname.getName().endsWith(".jar");
			}
		};

		/* iterate all jar */
		File[] allJars = new File(path).listFiles(jarFilter);
		List<URL> jarURLs = new ArrayList<URL>(allJars.length);

		for (int i = 0; i < allJars.length; i++) {
			try {
				jarURLs.add(allJars[i].toURI().toURL());
			} catch (Exception e) {
				throw new DataXException(
						FrameworkErrorCode.PLUGIN_INIT_ERROR,
						"System Fatal Error: Load Jar failed .", e);
			}
		}

		return jarURLs;
	}
}
