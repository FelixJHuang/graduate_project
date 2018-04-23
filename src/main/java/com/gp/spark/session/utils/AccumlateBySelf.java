package com.gp.spark.session.utils;

import com.gp.spark.session.domain.SessionStep;
import org.apache.spark.AccumulatorParam;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/15
 */
public class AccumlateBySelf implements AccumulatorParam<SessionStep> {

    //初始化方法
    @Override
    public SessionStep zero(SessionStep sessionStep) {
        sessionStep.setCount(0);
        sessionStep.setS1_s3(0);
        sessionStep.setS4_s6(0);
        sessionStep.setS7_s9(0);
        sessionStep.setS10_s30(0);
        sessionStep.setS30_s60(0);

        sessionStep.setM1_m3(0);
        sessionStep.setM10_m30(0);
        sessionStep.setM30(0);

        sessionStep.setStep1_3(0);
        sessionStep.setStep4_6(0);
        sessionStep.setStep7_9(0);
        sessionStep.setStep10_30(0);
        sessionStep.setStep30_60(0);
        sessionStep.setStep60(0);
        return sessionStep;
    }

    //累加方法
    public static SessionStep add(SessionStep s1, SessionStep s2) {
        if (s1 == null) {
            return s2;
        }
        //累加
        s1.setCount(s1.getCount() + s2.getCount());
        s1.setS1_s3(s1.getS1_s3() + s2.getS1_s3());
        s1.setS4_s6(s1.getS4_s6() + s2.getS4_s6());
        s1.setS7_s9(s1.getS7_s9() + s2.getS7_s9());
        s1.setS10_s30(s1.getS10_s30() + s2.getS10_s30());
        s1.setS30_s60(s1.getS30_s60() + s2.getS30_s60());

        s1.setM1_m3(s1.getM1_m3() + s2.getM1_m3());
        s1.setM10_m30(s1.getM10_m30() + s2.getM10_m30());
        s1.setM30(s1.getM30() + s2.getM30());

        s1.setStep1_3(s1.getStep1_3() + s2.getStep1_3());
        s1.setStep4_6(s1.getS4_s6() + s2.getStep4_6());
        s1.setStep7_9(s1.getStep7_9() + s2.getStep7_9());
        s1.setStep10_30(s1.getStep10_30() + s2.getStep10_30());
        s1.setStep30_60(s1.getStep30_60() + s2.getStep30_60());
        s1.setStep60(s1.getStep60() + s2.getStep60());
        return s1;
    }

    @Override
    public SessionStep addAccumulator(SessionStep t1, SessionStep t2) {
        return add(t1, t2);
    }

    @Override
    public SessionStep addInPlace(SessionStep r1, SessionStep r2) {
        return add(r1, r2);
    }


}
