package com.zy.bi.util;

import com.alibaba.fastjson.JSON;

/**
 * Created by allen on 2015/8/28.
 */
public class Message {

    private long ts; // timestamp offset
    private String op; // update/insert/delete
    private String ns; // namespace
    private String _id; // id offset
    private String o; // object content


    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getNs() {
        return ns;
    }

    public void setNs(String ns) {
        this.ns = ns;
    }

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getO() {
        return o;
    }

    public void setO(String o) {
        this.o = o;
    }


    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

}
