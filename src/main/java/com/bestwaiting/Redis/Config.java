package com.bestwaiting.Redis;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Config extends Properties {
	
	/**
	 * Created by bestwaiting on 2016/10/24
	 */
	private static final long serialVersionUID = 8082237910317527850L;
	private static Config instance = null;
    private Config() {
        InputStream in = ClassLoader.getSystemClassLoader().getResourceAsStream("config.xml");
        try {
            loadFromXML(in);
            in.close();
        } catch (IOException ioe) {

        }
    }
    public static synchronized Config getInstance() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    public String getProperty(String key, String defaultValue) {
        String val = getProperty(key);
        return (val == null || val.isEmpty()) ? defaultValue : val;
    }

    public String getString(String name, String defaultValue) {
        return this.getProperty(name, defaultValue);
    }
    
    public boolean getBoolean(String name, boolean defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Boolean.valueOf(val);
    }
    
    public int getInt(String name, int defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Integer.parseInt(val);
    }

    public long getLong(String name, long defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Integer.parseInt(val);
    }

    public float getFloat(String name, float defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Float.parseFloat(val);
    }

    public double getDouble(String name, double defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Double.parseDouble(val);
    }

    public byte getByte(String name, byte defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Byte.parseByte(val);
    }
}
