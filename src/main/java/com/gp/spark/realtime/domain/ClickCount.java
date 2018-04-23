package com.gp.spark.realtime.domain;

import java.io.Serializable;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/22
 */
public class ClickCount implements Serializable {
    private String date;
    private long userid;
    private long clickCount;
    private long adid;

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public long getUserid() {
        return userid;
    }

    public void setUserid(long userid) {
        this.userid = userid;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

    public long getAdid() {
        return adid;
    }

    public void setAdid(long adid) {
        this.adid = adid;
    }
}
