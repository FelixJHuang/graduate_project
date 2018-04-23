package com.gp.helper;

import com.gp.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * describe: 不使用框架自己去实现一个连接池，jdbchelper;查询插入组件
 *
 * @author huangjia
 * @date 2018/3/11
 */
public class JDBCManager {

    //单例对象
    private static JDBCManager instance = null;
    //连接池
    private LinkedList<Connection> dataSource = new LinkedList<Connection>();

    static {
        try {
            Class.forName(ConfManager.getPropertis(Constants.JDBC_DRIVER));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //创建单例模式的原因是保障我们内部创建的连接池只有一份
    public static JDBCManager getInstance() {
        if (instance == null) {
            synchronized (JDBCManager.class) {
                if (instance == null) {
                    instance = new JDBCManager();
                }
            }
        }

        return instance;
    }


    //私有化构造方法 创建连接池
    private JDBCManager() {
        int dataSourcesSize = ConfManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        for (int i = 0; i < dataSourcesSize; i++) {
            String url = ConfManager.getPropertis(Constants.JDBC_URL);
            String name = ConfManager.getPropertis(Constants.JDBC_USER);
            String password = ConfManager.getPropertis(Constants.JDBC_PASSWORD);
            try {
                Connection conn = DriverManager.getConnection(url, name, password);
                dataSource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //返回一个连接，并建立一个等待机制,synchronized是为了避免多线程并发访问的限制
    public synchronized Connection getConnection() {
        while (dataSource.size() == 0) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return dataSource.poll();
    }

    //增删改的方法
    public int executeUpdate(String sql, Object[] params) {
        int rs = 0;
        Connection conn = null;
        PreparedStatement pstat = null;

        try {
            conn = getConnection();
            pstat = conn.prepareStatement(sql);
            for (int i = 0; i < params.length; i++) {
                pstat.setObject(i + 1, params[i]);
            }
            rs = pstat.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
        return rs;
    }

    /**
     * @param sql
     * @param params
     * @param callBack 内部接口，自己实现查询结果的利用
     */
    public void exexuteQuery(String sql, Object[] params,
                             QueryCallBack callBack) {
        Connection conn = null;
        PreparedStatement pstat = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            pstat = conn.prepareStatement(sql);

            for (int i = 0; i < params.length; i++) {
                pstat.setObject(i + 1, params[i]);
            }
            rs = pstat.executeQuery();

            callBack.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
    }

    //批量插入
    public int[] exucuteBatch(String sql, List<Object[]> paramLists) {
        int[] rss = null;
        Connection conn = null;
        PreparedStatement pstat = null;

        try {
            conn = getConnection();
            conn.setAutoCommit(false); //禁止自动提交
            pstat = conn.prepareStatement(sql);
            for (Object[] params : paramLists) {
                for (int i = 0; i < params.length; i++) {
                    pstat.setObject(i + 1, params[i]);
                }
                pstat.addBatch();
            }
            rss = pstat.executeBatch();
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
        return rss;
    }

    //内部接口
    public interface QueryCallBack {
        void process(ResultSet rs) throws Exception;
    }
}
