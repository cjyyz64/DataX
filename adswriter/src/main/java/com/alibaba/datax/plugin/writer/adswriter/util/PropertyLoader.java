package com.alibaba.datax.plugin.writer.adswriter.util;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.Validate;

import java.net.URL;

/**
 * Created by judy.lt on 2015/2/27.
 */
public class PropertyLoader {
    private static Configuration configuration = null;

    static {
        try {
            URL url = PropertyLoader.class.getClassLoader().getResource("config.properties");
            PropertyLoader.configuration = new PropertiesConfiguration(url);
        } catch (ConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

    private static void checkState() {
        Validate.isTrue(!PropertyLoader.configuration.isEmpty(), "System Error: properties cannot be empty file !");
    }

    /**
     * Get a string associated with the given configuration key.
     *
     * @param key The configuration key.
     * @return The associated string.
     * @throws org.apache.commons.configuration.ConversionException is thrown if the key maps to an object that is not a String.
     */
    public static String getString(final String key) {
        checkState();
        return PropertyLoader.configuration.getString(key);
    }

    /**
     * Get a int associated with the given configuration key.
     *
     * @param key The configuration key.
     * @return The associated int.
     * @throws org.apache.commons.configuration.ConversionException is thrown if the key maps to an object that is not a Integer.
     */
    public static int getInt(final String key) {
        checkState();
        return PropertyLoader.configuration.getInt(key);
    }

    /**
     * Get a double associated with the given configuration key.
     *
     * @param key The configuration key.
     * @return The associated double.
     * @throws org.apache.commons.configuration.ConversionException is thrown if the key maps to an object that is not a Double.
     */
    public static double getDouble(final String key) {
        checkState();
        return PropertyLoader.configuration.getDouble(key);
    }

    public static boolean getBoolean(final String key){
        checkState();
        return PropertyLoader.configuration.getBoolean(key);
    }
}
