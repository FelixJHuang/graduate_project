package com.gp.daoImpl;

import com.gp.dao.ISessionDetailDAO;
import com.gp.helper.JDBCManager;
import com.gp.spark.session.domain.SessionDetail;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/20
 */
public class SessionDetailDAO implements ISessionDetailDAO {
    @Override
    public void insert(SessionDetail sessionDetail) {
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{
                sessionDetail.getTask_id(),
                sessionDetail.getUser_id(),
                sessionDetail.getSession_id(),
                sessionDetail.getPage_id(),
                sessionDetail.getAction_time(),
                sessionDetail.getSearch_keyword(),
                sessionDetail.getClick_category_id(),
                sessionDetail.getClick_product_id(),
                sessionDetail.getOrder_category_ids(),
                sessionDetail.getOrder_product_ids(),
                sessionDetail.getPay_category_ids(),
                sessionDetail.getPay_product_ids()};

        JDBCManager instance = JDBCManager.getInstance();
        instance.executeUpdate(sql, params);
    }
}
