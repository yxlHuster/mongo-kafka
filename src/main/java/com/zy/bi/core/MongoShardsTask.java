package com.zy.bi.core;

import com.google.common.collect.Lists;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoURI;
import com.zy.bi.util.MongoShardInfo;
import com.zy.bi.util.MongoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by allen on 2015/8/26.
 */
@SuppressWarnings("all")
public class MongoShardsTask implements Serializable  {

    private static final long serialVersionUID = 4440209304544126177L;
    private static Logger LOGGER = LoggerFactory.getLogger(MongoShardsTask.class);

    private LinkedBlockingQueue<DBObject> queue;
    // Keeps the running state
    private AtomicBoolean running = new AtomicBoolean(true);

    // Keeps the running thread index
    private AtomicInteger index = new AtomicInteger(0);

    // Mongo shards tasks
    private List<MongoTask> mongoTasks;

    private String dbName;
    private String[] collectionNames;

    // Filter namespaces not needed
    private String[] filterNameSpaces;

    public MongoShardsTask(LinkedBlockingQueue<DBObject> queue, String url, String dbName, String[] collectionNames, String[] filterNameSpaces) {
        this.queue = queue;
        this.dbName = dbName;
        this.collectionNames = collectionNames;
        this.filterNameSpaces = filterNameSpaces;
        initializeMongoShardTasks(url);
    }

    // Get shards from mongos
    private void initializeMongoShardTasks(String url) {
        // Open the db connection
        MongoURI uri = new MongoURI(url);
        // Create mongo instance
        Mongo mongos = new Mongo(uri);
        List<MongoShardInfo> shardInfos = MongoUtil.getMongoShards(mongos);
        mongoTasks = Lists.newArrayList();
        for (MongoShardInfo mongoShardInfo : shardInfos) {
            MongoTask mongoTask = new MongoTask(this.queue, mongoShardInfo.getHost(), this.dbName, this.collectionNames, this.filterNameSpaces);
            mongoTasks.add(mongoTask);
        }
        if (mongoTasks.size() == 0) {
            LOGGER.warn("no shards found, exit!");
            System.exit(-1);
        }
    }

    public void start() {
        for (MongoTask mongoTask : mongoTasks) {
            Thread thread = new Thread(mongoTask, "shardTaskThreads-" + index.getAndIncrement());
            thread.start();
        }
    }

    public void stop() {
        for (MongoTask mongoTask : mongoTasks) {
            mongoTask.stopThread();
        }
    }

}
