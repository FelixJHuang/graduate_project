package com.gp.daoImpl;

import com.gp.dao.IRandExtactSessionDao;
import com.gp.helper.ConfManager;
import com.gp.helper.JDBCManager;
import com.gp.spark.session.domain.SessionExtractRandom;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/20
 */
public class RandExtractSession implements IRandExtactSessionDao {
    @Override
    public void insert(SessionExtractRandom sessionExtractRandom) {
        String sql = "insert into session_random_extract "
                + "values(?,?,?,?,?)";
        Object[] param = new Object[]{
                sessionExtractRandom.getTask_id(),
                sessionExtractRandom.getSession_id(),
                sessionExtractRandom.getStart_time(),
                sessionExtractRandom.getCatagroy_id(),
                sessionExtractRandom.getSearch_keywords()
        };

        JDBCManager instance = JDBCManager.getInstance();
        instance.executeUpdate(sql, param);
    }
}
