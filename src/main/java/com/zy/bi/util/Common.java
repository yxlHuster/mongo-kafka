package com.zy.bi.util;

import org.apache.commons.lang3.StringUtils;

/**
 * Created by allen on 2015/8/28.
 */
public class Common {


    /**
     * return true if target in subs
     * @param target
     * @param subs
     * @return
     */
    public static boolean in_range(String target, String ...subs) {
        for (String sub : subs) {
            if (StringUtils.equals(target, sub)) return true;
        }
        return false;
    }

}
