package com.zy.bi.sinks;

/**
 * Created by allen on 2015/8/28.
 */
public class KafkaSinkConstants {

    public static final String PROPERTY_PREFIX = "kafka.";

    /* Properties */

    public static final String MESSAGE_SERIALIZER_KEY = "serializer.class";
    public static final String KEY_SERIALIZER_KEY = "key.serializer.class";
    public static final String BROKER_LIST_KEY = "metadata.broker.list";
    public static final String REQUIRED_ACKS_KEY = "request.required.acks";

    /* kafka config key from file */
    public static final String TOPIC = "topic";
    public static final String BATCH_SIZE = "batchSize";
    public static final String BROKER_LIST_CONF_KEY = "brokerList";
    public static final String REQUIRED_ACKS_CONF_KEY = "requiredAcks";
    public static final String QUEUE_SIZE = "queueSize";


    public static final int DEFAULT_BATCH_SIZE = 100;
    public static final String DEFAULT_TOPIC = "default-mongo-topic";
    public static final String DEFAULT_MESSAGE_SERIALIZER =
            "kafka.serializer.DefaultEncoder";
    public static final String DEFAULT_KEY_SERIALIZER =
            "kafka.serializer.StringEncoder";
    public static final String DEFAULT_REQUIRED_ACKS = "1";

    public static final int DEFAULT_QUEUE_SIZE = 10000;


    /* kafka mongo config */
    public static final String MONGO_DBNAME = "local";
    public static final String[] MONGO_COLLECTIONNAME = {"oplog.$main", "oplog.rs"};
    public static final String[] MONGO_FILTEREDNAMESPACE = {};
    public static final String MONGO_URI = "mongoUri";
    public static final String MONGO_MODE = "mongoMode";
    public static final String MONGO_FILTERED_NS = "filteredNameSpace";

}
