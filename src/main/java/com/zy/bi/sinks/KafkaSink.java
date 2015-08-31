package com.zy.bi.sinks;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import com.mongodb.DBObject;
import com.zy.bi.core.MongoShardsTask;
import com.zy.bi.core.MongoTask;
import com.zy.bi.util.Common;
import com.zy.bi.util.Message;
import com.zy.bi.util.MongoUtil;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by allen on 2015/8/28.
 */
public class KafkaSink extends AbstractSink {

    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaSink.class);

    // Kafka config
    private Properties kafkaProps;
    private Producer<String, byte[]> producer;
    private String topic;
    private int batchSize;
    private List<KeyedMessage<String, byte[]>> messageList;

    // Inner queue config
    private LinkedBlockingQueue<DBObject> queue;
    private int queueSize;

    // Mongo config
    private MongoTask mongoTask;
    private MongoShardsTask mongoShardsTask;
    private MongoStatus mongoStatus;
    private String mongoUri;
    private String[] filteredNs;

    // Keeps the running state
    private AtomicBoolean running = new AtomicBoolean(true);

    @Override
    public void process() {
        // While the thread is set to running
        while (running.get()) {
            try {
                long msgSize = 0;
                messageList.clear();
                for (; msgSize < batchSize; msgSize++) {
                    DBObject object = queue.take();
                    if (object == null) break;
                    Message message = MongoUtil.transOpLog2Message(object.toString());
                    if (message == null) continue;
                    // Create a message and add to buffer
                    KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>
                            (topic, message.get_id(), message.toString().getBytes("UTF-8"));
                    messageList.add(data);
                }
                if (msgSize > 0) {
                    producer.send(messageList);
                } else {
                    // Sleep for 50 ms and then wake up
                    Thread.sleep(50);
                }
            } catch (Exception e) {
                if (running.get()) throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void configure(String configFile) {

        Properties conf = new Properties();
        try {
            conf.load(new FileInputStream(configFile));
        } catch (Exception e) {
            LOGGER.warn("config file not found!");
        }
        String batch = conf.getProperty(KafkaSinkConstants.BATCH_SIZE);
        if (batch == null) {
            batchSize = KafkaSinkConstants.DEFAULT_BATCH_SIZE;
        } else {
            batchSize = Ints.tryParse(batch);
        }
        messageList =
                new ArrayList<KeyedMessage<String, byte[]>>(batchSize);
        LOGGER.debug("Using batch size: {}", batchSize);
        topic = conf.getProperty(KafkaSinkConstants.TOPIC,
                KafkaSinkConstants.DEFAULT_TOPIC);
        if (topic.equals(KafkaSinkConstants.DEFAULT_TOPIC)) {
            LOGGER.warn("The Property 'topic' is not set. " +
                    "Using the default topic name: " +
                    KafkaSinkConstants.DEFAULT_TOPIC);
        } else {
            LOGGER.info("Using the static topic: " + topic +
                    " this may be over-ridden by event headers");
        }
        kafkaProps = KafkaSinkUtil.getKafkaProperties(conf);
        String qSize = conf.getProperty(KafkaSinkConstants.QUEUE_SIZE);
        if (qSize == null) {
            queueSize = KafkaSinkConstants.DEFAULT_QUEUE_SIZE;
        } else {
            queueSize = Ints.tryParse(qSize);
        }
        LOGGER.debug("Using batch size: {}", queueSize);
        queue = new LinkedBlockingQueue<DBObject>(queueSize);

        mongoUri = conf.getProperty(KafkaSinkConstants.MONGO_URI);
        Preconditions.checkNotNull(mongoUri);
        if (!mongoUri.startsWith("mongodb")) {
            mongoUri = "mongodb://" + mongoUri;
        }
        String mongoMode = conf.getProperty(KafkaSinkConstants.MONGO_MODE);
        Preconditions.checkNotNull(mongoMode);
        Preconditions.checkState(Common.in_range(mongoMode, "shard", "master-slave"), "mongoMode should be shard/master-slave");
        if (StringUtils.equals(mongoMode, "shard")) {
            mongoStatus = MongoStatus.SHARD;
        } else {
            mongoStatus = MongoStatus.MASS;
        }
        String filteredNameSpaces = conf.getProperty(KafkaSinkConstants.MONGO_FILTERED_NS);
        if (StringUtils.isNotBlank(filteredNameSpaces)) {
            filteredNs = filteredNameSpaces.split(",");
        } else {
            filteredNs = KafkaSinkConstants.MONGO_FILTEREDNAMESPACE;
        }

    }

    @Override
    public synchronized void start() {
        // Init the producer
        ProducerConfig config = new ProducerConfig(kafkaProps);
        producer = new Producer<String, byte[]>(config);

        // Init mongo task
        if (mongoStatus == MongoStatus.MASS) {
            mongoTask = new MongoTask(queue, mongoUri, KafkaSinkConstants.MONGO_DBNAME, KafkaSinkConstants.MONGO_COLLECTIONNAME, filteredNs);
            Thread thread = new Thread(mongoTask);
            thread.start();
        } else {
            mongoShardsTask = new MongoShardsTask(queue, mongoUri, KafkaSinkConstants.MONGO_DBNAME, KafkaSinkConstants.MONGO_COLLECTIONNAME, filteredNs);
            mongoShardsTask.start();
        }
        // Start to process message in queue
        process();
    }

    @Override
    public synchronized void stop() {
        producer.close();
        if (mongoStatus == MongoStatus.MASS) {
            mongoTask.stopThread();
        } else {
            mongoShardsTask.stop();
        }
        running.set(false);
    }

    public static enum MongoStatus {
        SHARD, MASS // master-slave
    }
}
