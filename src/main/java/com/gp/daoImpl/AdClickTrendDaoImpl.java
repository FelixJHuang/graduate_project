package com.gp.daoImpl;

import com.gp.dao.IAdClickTrendDAO;
import com.gp.helper.JDBCManager;
import com.gp.spark.realtime.domain.AdClickTrend;
import com.gp.spark.realtime.domain.AdClickTrendQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.WeakHashMap;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/23
 */
public class AdClickTrendDaoImpl implements IAdClickTrendDAO {
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrends) {
        JDBCManager jdbcHelper = JDBCManager.getInstance();

        // 区分出来哪些数据是要插入的，哪些数据是要更新的

        List<AdClickTrend> updateAdClickTrends = new ArrayList<AdClickTrend>();
        List<AdClickTrend> insertAdClickTrends = new ArrayList<AdClickTrend>();

        String selectSQL = "SELECT count(*) "
                + "FROM ad_click_trend "
                + "WHERE date=? "
                + "AND hour=? "
                + "AND minute=? "
                + "AND ad_id=?";

        for (AdClickTrend adClickTrend : adClickTrends) {
            final AdClickTrendQueryResult al = new AdClickTrendQueryResult();

            Object[] params = new Object[]{adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAddid()};

            jdbcHelper.exexuteQuery(selectSQL, params, new JDBCManager.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    while (rs.next()) {
                        if (rs.next()) {
                            int count = rs.getInt(1);
                            al.setCount(count);
                        }
                    }
                }
            });

            int count = al.getCount();
            if (count > 0) {
                updateAdClickTrends.add(adClickTrend);
            } else {
                insertAdClickTrends.add(adClickTrend);
            }
        }

        // 执行批量更新操作
        String updateSQL = "UPDATE ad_click_trend SET click_count=? "
                + "WHERE date=? "
                + "AND hour=? "
                + "AND minute=? "
                + "AND ad_id=?";

        List<Object[]> updateParamsList = new ArrayList<Object[]>();

        for (AdClickTrend adClickTrend : updateAdClickTrends) {
            Object[] params = new Object[]{adClickTrend.getClickCount(),
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAddid()};
            updateParamsList.add(params);
        }

        jdbcHelper.exucuteBatch(updateSQL, updateParamsList);

        // 执行批量更新操作
        String insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)";

        List<Object[]> insertParamsList = new ArrayList<Object[]>();

        for (AdClickTrend adClickTrend : insertAdClickTrends) {
            Object[] params = new Object[]{adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAddid(),
                    adClickTrend.getClickCount()};
            insertParamsList.add(params);
        }

        jdbcHelper.exucuteBatch(insertSQL, insertParamsList);
    }
}
