package com.gp.daoImpl;

import com.google.common.collect.Lists;
import com.gp.dao.ClickCountDao;
import com.gp.helper.JDBCManager;
import com.gp.spark.realtime.domain.AdUserClickCountQueryResult;
import com.gp.spark.realtime.domain.ClickCount;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * describe: 更新数据库或者插入新的数据
 *
 * @author huangjia
 * @date 2018/3/22
 */
public class ClickCountDaoImpl implements ClickCountDao {
    @Override
    public void updateBatch(List<ClickCount> clickCounts) {
        List<ClickCount> insertClick = Lists.newArrayList();
        List<ClickCount> updateClick = Lists.newArrayList();

        JDBCManager instance = JDBCManager.getInstance();
        //统计是否存在这个user点击情况 count>0就是存在，那么我们就是更新操作，
        // 如果count等于0，那么我们就执行插入操作
        String sql = "SELECT count(*) FROM ad_user_click_count "
                + "WHERE date=? AND user_id=? AND ad_id=? ";
        Object[] paramsselect = null;
        for (ClickCount clickCount : clickCounts) {
            final AdUserClickCountQueryResult result = new AdUserClickCountQueryResult();
            paramsselect = new Object[]{
                    clickCount.getDate(),
                    clickCount.getUserid(),
                    clickCount.getAdid()
            };

            instance.exexuteQuery(sql, paramsselect, new JDBCManager.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    int count = rs.getInt(1);
                    result.setCount(count);
                }
            });


            int count = result.getCount();
            if (count > 0) {
                updateClick.add(clickCount);
            } else {
                insertClick.add(clickCount);
            }
        }

        //插入
        String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";
        Object[] paramInsert = null;
        List<Object[]> listObject = Lists.newArrayList();
        for (ClickCount clickCount : insertClick) {
            paramInsert = new Object[]{
                    clickCount.getDate(),
                    clickCount.getUserid(),
                    clickCount.getAdid(),
                    clickCount.getClickCount()
            };

            listObject.add(paramInsert);
        }
        instance.exucuteBatch(insertSQL, listObject);

        //更新
        String updateSQL = "UPDATE ad_user_click_count SET click_count=? "
                + "WHERE date=? AND user_id=? AND ad_id=? ";
        List<Object[]> updateParamsList = new ArrayList<Object[]>();

        for (ClickCount adUserClickCount : updateClick) {
            Object[] updateParams = new Object[]{adUserClickCount.getClickCount(),
                    adUserClickCount.getDate(),
                    adUserClickCount.getUserid(),
                    adUserClickCount.getAdid()};
            updateParamsList.add(updateParams);
        }

        instance.exucuteBatch(updateSQL, updateParamsList);
    }

    @Override
    public int findClickCountByMultiKey(String date, long userid, long adid) {
        String sql = "SELECT click_count "
                + "FROM ad_user_click_count "
                + "WHERE date=? "
                + "AND user_id=? "
                + "AND ad_id=?";

        Object[] params = new Object[]{date, userid, adid};

        final List<Integer> queryResult = new ArrayList<Integer>();

        JDBCManager jdbcHelper = JDBCManager.getInstance();
        jdbcHelper.exexuteQuery(sql, params, new JDBCManager.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    int clickCount = rs.getInt(1);
                    queryResult.add(clickCount);
                }
            }
        });

        int clickCount = queryResult.get(0);

        return clickCount;
    }
}
