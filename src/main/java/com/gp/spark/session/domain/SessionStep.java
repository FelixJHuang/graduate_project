package com.gp.spark.session.domain;

import java.io.Serializable;

/**
 * describe: 累加对象 设置各个变量的值
 *
 * @author huangjia
 * @date 2018/3/15
 */
public class SessionStep implements Serializable {
    private long count;
    private long s1_s3;
    private long s4_s6;
    private long s7_s9;
    private long s10_s30;
    private long s30_s60;
    private long m1_m3;
    private long m10_m30;
    private long m30;
    private long step1_3;
    private long step4_6;
    private long step7_9;
    private long step10_30;
    private long step30_60;
    private long step60;

    public SessionStep() {
    }

    public SessionStep(long count, long s1_s3, long s4_s6, long s7_s9, long s10_s30, long s30_s60,
                       long m1_m3, long m10_m30, long m30, long step1_3, long step4_6, long step7_9,
                       long step10_30, long step30_60, long step60) {
        this.count = count;
        this.s1_s3 = s1_s3;
        this.s4_s6 = s4_s6;
        this.s7_s9 = s7_s9;
        this.s10_s30 = s10_s30;
        this.s30_s60 = s30_s60;
        this.m1_m3 = m1_m3;
        this.m10_m30 = m10_m30;
        this.m30 = m30;
        this.step1_3 = step1_3;
        this.step4_6 = step4_6;
        this.step7_9 = step7_9;
        this.step10_30 = step10_30;
        this.step30_60 = step30_60;
        this.step60 = step60;
    }

    public long getM10_m30() {
        return m10_m30;
    }

    public void setM10_m30(long m10_m30) {
        this.m10_m30 = m10_m30;
    }


    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public long getS1_s3() {
        return s1_s3;
    }

    public void setS1_s3(long s1_s3) {
        this.s1_s3 = s1_s3;
    }

    public long getS4_s6() {
        return s4_s6;
    }

    public void setS4_s6(long s4_s6) {
        this.s4_s6 = s4_s6;
    }

    public long getS7_s9() {
        return s7_s9;
    }

    public void setS7_s9(long s7_s9) {
        this.s7_s9 = s7_s9;
    }

    public long getS10_s30() {
        return s10_s30;
    }

    public void setS10_s30(long s10_s30) {
        this.s10_s30 = s10_s30;
    }

    public long getS30_s60() {
        return s30_s60;
    }

    public void setS30_s60(long s30_s60) {
        this.s30_s60 = s30_s60;
    }

    public long getM1_m3() {
        return m1_m3;
    }

    public void setM1_m3(long m1_m3) {
        this.m1_m3 = m1_m3;
    }


    public long getM30() {
        return m30;
    }

    public void setM30(long m30) {
        this.m30 = m30;
    }

    public long getStep1_3() {
        return step1_3;
    }

    public void setStep1_3(long step1_3) {
        this.step1_3 = step1_3;
    }

    public long getStep4_6() {
        return step4_6;
    }

    public void setStep4_6(long step4_6) {
        this.step4_6 = step4_6;
    }

    public long getStep7_9() {
        return step7_9;
    }

    public void setStep7_9(long step7_9) {
        this.step7_9 = step7_9;
    }

    public long getStep10_30() {
        return step10_30;
    }

    public void setStep10_30(long step10_30) {
        this.step10_30 = step10_30;
    }

    public long getStep30_60() {
        return step30_60;
    }

    public void setStep30_60(long step30_60) {
        this.step30_60 = step30_60;
    }

    public long getStep60() {
        return step60;
    }

    public void setStep60(long step60) {
        this.step60 = step60;
    }
}
