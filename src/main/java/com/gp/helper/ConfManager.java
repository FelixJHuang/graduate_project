package com.gp.helper;

import java.io.InputStream;
import java.util.Properties;

/**
 * describe:配置管理组件
 *
 * @author huangjia
 * @date 2018/3/11
 */
public class ConfManager {
    private static Properties prop = new Properties();

    //第一次加载类的时候就会首先初始化,并且只会加载一次,这样我们的效率比较高,所以我们在这里区读取配置文件
    static {
        try {
            //name：就是我们放在resoucres中配置文件名称
            InputStream in = ConfManager.class.getClassLoader().getResourceAsStream("constant.properties");
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param key 配置文件中的key
     * @return 配置文件中int类型
     */
    public static String getPropertis(String key) {
        return prop.getProperty(key);
    }

    public static Integer getInteger(String key) {
        String value = getPropertis(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * 得到布尔值
     *
     * @param key
     * @return
     */
    public static Boolean getBoolean(String key) {
        String value = getPropertis(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 得到Long类型的value值
     *
     * @param key
     * @return
     */
    public static Long getLong(String key) {
        String value = getPropertis(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0l;
    }
}
