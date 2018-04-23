package com.gp.jdbc.test;

import java.sql.*;

/**
 * describe:实现一些增删改查的功能，不会去使用那些框架，
 * 这样会加到我们项目成本，实际工程中使用那些比如mybatis hibernate sping这些框架
 *
 * @author huangjia
 * @date 2018/3/11
 */
public class JDBCTest {

    public static void main(String[] args) {
        //insert();
        //update();
        //delete();
        query();
    }

    /**
     * 插入数据
     */
    public static void insert() {
        Connection conn = null;
        Statement stat = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.0.110:3306/test?useUnicode=true&characterEncoding=UTF-8", "hive", "hive");
            stat = conn.createStatement();
            String sql = "insert into test_user(name,age) values('魏瑶',22)";
            int b = stat.executeUpdate(sql);
            System.out.println("b=" + b);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //更新操作
    private static void update() {
        Connection conn = null;
        Statement stat = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.0.110:3306/test?useUnicode=true&characterEncoding=UTF-8", "hive", "hive");
            stat = conn.createStatement();
            String sql = "update test_user set age=23 where name='魏瑶'";
            stat.executeUpdate(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private static void delete() {
        Connection conn = null;
        Statement stat = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.0.110:3306/test?useUnicode=true&characterEncoding=UTF-8", "hive", "hive");
            stat = conn.createStatement();
            String sql = "DELETE FROM test_user WHERE name='张三'";
            stat.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    private static void query() {
        Connection conn = null;
        Statement stat = null;
        ResultSet rs = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection("jdbc:mysql://192.168.0.110:3306/test?useUnicode=true&characterEncoding=UTF-8", "hive", "hive");
            stat = conn.createStatement();
            String sql = "SELECT * FROM test_user";
            rs = stat.executeQuery(sql);
            while (rs.next()) {
                int id = rs.getInt(1);
                String name = rs.getString(2);
                int age = rs.getInt(3);
                System.out.println("id=" + id + ",name=" + name + ",age=" + age);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (stat != null) {
                try {
                    stat.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    //以上的方法可能会导致SQL注入，后面我们会使用perparstatment代替
}
