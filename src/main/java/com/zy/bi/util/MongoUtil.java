package com.zy.bi.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.Mongo;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by allen on 2015/8/26.
 */
public class MongoUtil {


    /**
     * get mongo shards info using admin db
     * @param mongo
     * @return
     */
    @SuppressWarnings("all")
    public static List<MongoShardInfo> getMongoShards(Mongo mongo) {
        List<MongoShardInfo> shardInfos = Lists.newArrayList();
        Map<String, String> params = new HashMap<String, String>();
        params.put("listshards", "1");
        CommandResult result = mongo.getDB("admin").command(new BasicDBObject(params));
        if (result != null && result.containsKey("shards")) {
            String shards = result.getString("shards");
            if (StringUtils.isNotBlank(shards)) {
                List<MongoShardInfo> mongoShardInfos = JSON.parseArray(shards, MongoShardInfo.class);
                for (MongoShardInfo mongoShardInfo : mongoShardInfos) {
                    MongoShardInfo shardInfo = new MongoShardInfo();
                    shardInfo.set_id(mongoShardInfo.get_id());
                    String host = "mongodb://" + StringUtils.split(mongoShardInfo.getHost(), "/")[1];
                    shardInfo.setHost(host);
                    shardInfos.add(shardInfo);
                }

            }
        }
        return shardInfos;
    }

    /**
     * transform mongo oplog to message for delivery
     * @param oplog
     * @return
     */
    public static Message transOpLog2Message(String oplog) {
        if (StringUtils.isBlank(oplog)) return null;
        try {
            JSONObject obj = JSON.parseObject(oplog);
            JSONObject timeObj = obj.getJSONObject("ts");
            Preconditions.checkNotNull(timeObj);
            String time = timeObj.getString("$ts");
            if (StringUtils.isBlank(time)) return null;
            long ts = Longs.tryParse(time);
            String op = obj.getString("op");
            if (!Common.in_range(op, "i", "u", "d")) return null;
            String ns = obj.getString("ns");
            if (StringUtils.isBlank(ns)) return null;
            JSONObject o2 = obj.getJSONObject("o2");
            if (o2 == null) return null;
            String _id = o2.getString("_id");
            if (StringUtils.isBlank(_id)) return null;
            String o = obj.getString("o");
            Message message = new Message();
            message.setTs(ts);
            message.setOp(op);
            message.setNs(ns);
            message.set_id(_id);
            message.setO(o);
            return message;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
