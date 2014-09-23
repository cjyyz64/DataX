package com.alibaba.datax.core.util;

import java.lang.reflect.Constructor;

public final class ClassUtil {

	/**
	 * 通过反射构造类对象
	 * 
	 * @param className
	 *            反射的类名称
	 * @param t
	 *            反射类的类型Class对象
	 * @param args
	 *            构造参数
	 * 
	 * */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static <T> T instantiate(String className, Class<T> t,
			Object... args) {
		try {
			Constructor constructor = (Constructor) Class.forName(className)
					.getConstructor(ClassUtil.toClassType(args));
			return (T) constructor.newInstance(args);
		} catch (Exception e) {
			throw new IllegalArgumentException(e);
		}
	}

	private static Class<?>[] toClassType(Object[] args) {
		Class<?>[] clazzs = new Class<?>[args.length];

		for (int i = 0, length = args.length; i < length; i++) {
			clazzs[i] = args[i].getClass();
		}

		return clazzs;
	}

	// @SuppressWarnings("unchecked")
	// public static <T> T instantiate(final String className, T t, Object...
	// args) {
	// return (T) ClassUtils.instantiate(className, args);
	// }

	// @SuppressWarnings({ "rawtypes" })
	// public static Object instantiate(final String className,
	// final Configuration configuration) {
	// try {
	// Constructor constructor = (Constructor) Class.forName(className)
	// .getConstructor(Configuration.class);
	// return constructor.newInstance(configuration);
	// } catch (Exception e) {
	// throw new IllegalArgumentException(e);
	// }
	// }
	//
	// @SuppressWarnings("unchecked")
	// public static <T> T instantiate(final String className,
	// final Configuration configuration, Class<T> t) {
	// return (T) ClassUtils.instantiate(className, configuration);
	// }
	//
	// @SuppressWarnings({ "rawtypes" })
	// public static <P> Object instantiate(final String clazz,
	// final Configuration configuration, final P param) {
	// try {
	// Constructor constructor = (Constructor) Class.forName(clazz)
	// .getConstructor(Configuration.class, param.getClass());
	// return constructor.newInstance(configuration, param);
	// } catch (Exception e) {
	// throw new IllegalArgumentException(e);
	// }
	// }
	//
	// @SuppressWarnings({ "unchecked" })
	// public static <T, P> T instantiate(final String clazz, Class<T> t,
	// final Configuration configuration, final P param) {
	// try {
	// return (T) ClassUtils.instantiate(clazz, configuration, param);
	// } catch (Exception e) {
	// throw new IllegalArgumentException(e);
	// }
	// }
	//
	// @SuppressWarnings({ "rawtypes" })
	// public static <P, V> Object instantiate(final String clazz,
	// final Configuration configuration, final P param, final V var) {
	// try {
	// Constructor constructor = (Constructor) Class.forName(clazz)
	// .getConstructor(Configuration.class, param.getClass(),
	// var.getClass());
	// return constructor.newInstance(configuration, param, var);
	// } catch (Exception e) {
	// throw new IllegalArgumentException(e);
	// }
	// }
	//
	// @SuppressWarnings({ "unchecked" })
	// public static <T, P, V> T instantiate(final String clazz, Class<T> t,
	// final Configuration configuration, final P param, final V var) {
	// try {
	// return (T) ClassUtils.instantiate(clazz, configuration, param, var);
	// } catch (Exception e) {
	// throw new IllegalArgumentException(e);
	// }
	// }

}
