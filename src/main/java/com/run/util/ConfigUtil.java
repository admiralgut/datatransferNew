package com.run.util;

import com.run.DataTransfer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Set;

public class ConfigUtil {
    private static final Logger log = LoggerFactory.getLogger(ConfigUtil.class);
    private static final Properties props = new Properties();

    static {
        InputStream in = DataTransfer.class.getResourceAsStream("/channel-config.properties");
        try {
            props.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getProperty(String name) {
        if (!props.containsKey(name)) {
            throw new RuntimeException("no such property: " + name);
        } else {
            return props.getProperty(name);
        }
    }

    public static String getProperty(String name, String defaultValue) {
        return props.getProperty(name, defaultValue);
    }

    public static boolean hasProperty(String name) {
        return props.containsKey(name);
    }

    public static int getInt(String name) {
        if (!props.containsKey(name)) {
            throw new RuntimeException("no such property: " + name);
        } else {
            return Integer.parseInt(props.getProperty(name));
        }
    }

    public static long getLong(String name) {
        if (!props.containsKey(name)) {
            throw new RuntimeException("no such property: " + name);
        } else {
            return Long.parseLong(props.getProperty(name));
        }
    }

    public static boolean getBoolean(String name) {
        if (!props.containsKey(name)) {
            throw new RuntimeException("no such property: " + name);
        } else {
            return Boolean.parseBoolean(props.getProperty(name));
        }
    }

    public static void setProperty(String name, String value) {
        props.setProperty(name, value);
    }

    public static Set<String> getPropertyNames() {
        return props.stringPropertyNames();
    }

    public static String[] getMonitorHosts() {
        return props.getProperty("monitor.hosts").split(",");
    }

    public static String getHostIp() {
        String ip = getProperty("host.ip", "127.0.0.1");
        if (StringUtils.isBlank(ip) || ip.equals("127.0.0.1")) {
            InetAddress addr = null;
            try {
                addr = InetAddress.getLocalHost();
                ip = addr.getHostAddress(); //获得机器IP
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        log.info("getHostIp: {}", ip);
        return ip;
    }

    public static long getInitialDelay(int hour, int minute, int second) {
        SimpleDateFormat dfFull = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        Calendar cdar = Calendar.getInstance();
        cdar.set(Calendar.HOUR_OF_DAY, hour);
        cdar.set(Calendar.MINUTE, minute);
        cdar.set(Calendar.SECOND, second);
        cdar.set(Calendar.MILLISECOND, 0);
        long millis = cdar.getTimeInMillis();
        long now = System.currentTimeMillis();
        if (millis < now) {
            cdar.add(Calendar.DATE, 1);
            millis = cdar.getTimeInMillis();
        }
        long diff = millis - now;
        log.info("getInitialDelay# now=" + now + "/" + dfFull.format(new Date(now)) //
                + ", next=" + millis + "/" + dfFull.format(cdar.getTime()) + ", diff=" + diff);
        return diff;
    }

}
