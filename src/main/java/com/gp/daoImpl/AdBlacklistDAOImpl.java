package com.gp.daoImpl;

import com.gp.dao.IAdBlacklistDAO;
import com.gp.spark.realtime.domain.AdBlacklist;
import com.gp.helper.JDBCManager;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/23
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    @Override
    public void insertBatch(List<AdBlacklist> adBlacklists) {
        String sql = "INSERT INTO ad_blacklist VALUES(?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();

        for (AdBlacklist adBlacklist : adBlacklists) {
            Object[] params = new Object[]{adBlacklist.getUserid()};
            paramsList.add(params);
        }

        JDBCManager jdbcHelper = JDBCManager.getInstance();
        jdbcHelper.exucuteBatch(sql, paramsList);
    }

    @Override
    public List<AdBlacklist> findAll() {
        String sql = "SELECT * FROM ad_blacklist";

        final List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

        JDBCManager jdbcHelper = JDBCManager.getInstance();

        jdbcHelper.exexuteQuery(sql, null, new JDBCManager.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    long userid = Long.valueOf(String.valueOf(rs.getInt(1)));

                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserid(userid);

                    adBlacklists.add(adBlacklist);
                }
            }
        });

        return adBlacklists;
    }


}
