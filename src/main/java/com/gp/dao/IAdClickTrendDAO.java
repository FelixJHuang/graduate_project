package com.gp.dao;

import com.gp.spark.realtime.domain.AdClickTrend;

import java.util.List;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/23
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
