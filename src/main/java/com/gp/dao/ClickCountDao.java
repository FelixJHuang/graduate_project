package com.gp.dao;

import com.gp.spark.realtime.domain.ClickCount;

import java.util.List;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/22
 */
public interface ClickCountDao {
    void updateBatch(List<ClickCount> clickCounts);

    int findClickCountByMultiKey(String date, long userid, long adid);

}
