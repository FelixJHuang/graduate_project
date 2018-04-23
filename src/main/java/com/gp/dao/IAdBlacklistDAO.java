package com.gp.dao;

import com.gp.spark.realtime.domain.AdBlacklist;

import java.util.List;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/23
 */
public interface IAdBlacklistDAO {
    void insertBatch(List<AdBlacklist> adBlacklists);

    List<AdBlacklist> findAll();
}
