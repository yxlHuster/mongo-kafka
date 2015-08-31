package com.zy.bi.util;

/**
 * Created by allen on 2015/8/26.
 */
public class MongoShardInfo {

    private String _id;
    private String host;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
