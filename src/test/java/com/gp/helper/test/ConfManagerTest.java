package com.gp.helper.test;

import com.gp.helper.ConfManager;

/**
 * describe:
 *
 * @author huangjia
 * @date 2018/3/11
 */
public class ConfManagerTest {
    public static void main(String[] args) {
        String s = ConfManager.getPropertis("test1");
        System.out.println(s);
    }
}
