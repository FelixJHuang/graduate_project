package com.gp.dao;

import com.gp.spark.session.domain.Top10Entity;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/21
 */
public interface ITop10DAO {
    /**
     *
     * @param top10Entity
     */
    void insert(Top10Entity top10Entity);
}
