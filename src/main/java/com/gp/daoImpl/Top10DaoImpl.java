package com.gp.daoImpl;

import com.gp.dao.ITop10DAO;
import com.gp.helper.JDBCManager;
import com.gp.spark.session.domain.Top10Entity;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/21
 */
public class Top10DaoImpl implements ITop10DAO {
    @Override
    public void insert(Top10Entity top10Entity) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{top10Entity.getTaskid(),
                top10Entity.getCategoryid(),
                top10Entity.getClickCount(),
                top10Entity.getOrderCount(),
                top10Entity.getPayCount()};

        JDBCManager jdbcHelper = JDBCManager.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
