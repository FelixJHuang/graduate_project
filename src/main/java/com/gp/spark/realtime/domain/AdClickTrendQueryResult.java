package com.gp.spark.realtime.domain;

import java.io.Serializable;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/23
 */
public class AdClickTrendQueryResult implements Serializable {
    private int count;

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }
}
