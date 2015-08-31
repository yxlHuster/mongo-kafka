package com.zy.bi.core;

import com.google.common.collect.Sets;
import com.mongodb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by allen on 2015/8/25.
 */
@SuppressWarnings("all")
public class MongoTask implements Callable<Boolean>, Runnable, Serializable {
    private static final long serialVersionUID = 4440209304544126477L;
    private static Logger LOGGER = LoggerFactory.getLogger(MongoTask.class);

    private LinkedBlockingQueue<DBObject> queue;
    private Mongo mongo;
    private DB db;
    private DBCollection collection;
    private DBCursor cursor;


    // Keeps the running state
    private AtomicBoolean running = new AtomicBoolean(true);
    private String[] collectionNames;
    private DBObject query;

    // Filter namespaces not needed
    private Set<String> filterNameSpaces;

    public MongoTask(LinkedBlockingQueue<DBObject> queue, String url, String dbName, String[] collectionNames, String[] filterNameSpaces) {
        this.queue = queue;
        this.collectionNames = collectionNames;
        this.filterNameSpaces = Sets.newHashSet(filterNameSpaces);
        initializeMongo(url, dbName);
    }

    private void initializeMongo(String url, String dbName) {
        // Open the db connection
        MongoURI uri = new MongoURI(url);
        // Create mongo instance
        this.mongo = new Mongo(uri);
        // Get the db the user wants
        db = mongo.getDB(dbName == null ? uri.getDatabase() : dbName);
        this.query = findLastOpLogEntry();
    }

    public void stopThread() {
        running.set(false);
    }

    public Boolean call() throws Exception {
        String collectionName = locateValidOpCollection(collectionNames);
        if (collectionName == null)
            throw new Exception("Could not locate any of the collections provided or not capped collection");
        // Set up the collection
        this.collection = this.db.getCollection(collectionName);

        // provide the query object
        this.cursor = this.collection.find(query)
                .sort(new BasicDBObject("$natural", 1))
                .addOption(Bytes.QUERYOPTION_TAILABLE)
                .addOption(Bytes.QUERYOPTION_AWAITDATA)
                .addOption(Bytes.QUERYOPTION_NOTIMEOUT);

        // While the thread is set to running
        while (running.get()) {
            try {
                // Check if we have a next item in the collection
                if (this.cursor.hasNext()) {
                    // Fetch the next object and push it on the queue
                    DBObject object = this.cursor.next();
                    // Verify if it's the filtered namespace
                    if (!this.filterNameSpaces.isEmpty() && this.filterNameSpaces.contains(object.get("ns").toString())) {
                        continue;
                    }
                    this.queue.put(object);
                } else {
                    // Sleep for 50 ms and then wake up
                    Thread.sleep(50);
                }
            } catch (Exception e) {
                if (running.get()) throw new RuntimeException(e);
            }
        }
        // Dummy return
        return true;
    }

    private String locateValidOpCollection(String[] collectionNames) {
        // Find a valid collection (used for oplogs etc)
        String collectionName = null;
        for (int i = 0; i < collectionNames.length; i++) {
            String name = collectionNames[i];
            // Attempt to read from the collection
            DBCollection collection = this.db.getCollection(name);
            // Attempt to find the last item in the collection
            DBCursor lastCursor = collection.find().sort(new BasicDBObject("$natural", -1)).limit(1);
            if (lastCursor != null && lastCursor.hasNext()) {
                collectionName = name;
                break;
            }
        }
        // return the collection name
        return collectionName;
    }

    protected DBObject findLastOpLogEntry() {
        // Connect to the db and find the current last timestamp
        DBObject query = null;
        DBCursor cursor = db.getCollection("oplog.rs").find().sort(new BasicDBObject("$natural", -1)).limit(1);
        if (cursor.hasNext()) {
            // Get the next object
            DBObject object = cursor.next();
            // Build the query
            query = new BasicDBObject("ts", new BasicDBObject("$gt", object.get("ts")));
        }
        // Return the query to find the last op log entry
        return query;
    }

    public void run() {
        try {
            call();
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}

