package com.zy.bi.sinks;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

/**
 * Created by allen on 2015/8/28.
 */
public class KafkaSinkUtil {


    public static final Logger LOGGER = LoggerFactory.getLogger(KafkaSinkUtil.class);

    /**
     * Get kafka producer properties
     * @return
     */
    public static Properties getKafkaProperties(Properties conf) {
        Properties props =  generateDefaultKafkaProps();
        addDocumentedKafkaProps(conf, props);
        return props;
    }


    /**
     * Generate producer properties object with some defaults
     * @return
     */
    private static Properties generateDefaultKafkaProps() {
        Properties props = new Properties();
        props.put(KafkaSinkConstants.MESSAGE_SERIALIZER_KEY,
                KafkaSinkConstants.DEFAULT_MESSAGE_SERIALIZER);
        props.put(KafkaSinkConstants.KEY_SERIALIZER_KEY,
                KafkaSinkConstants.DEFAULT_KEY_SERIALIZER);
        props.put(KafkaSinkConstants.REQUIRED_ACKS_KEY,
                KafkaSinkConstants.DEFAULT_REQUIRED_ACKS);
        return props;
    }


    /**
     * Add configurable settings
     * @param kafkaProps
     */
    private static void addDocumentedKafkaProps(Properties conf, Properties kafkaProps) {
        String brokerList = conf.getProperty(
                KafkaSinkConstants.BROKER_LIST_CONF_KEY);
        if (brokerList == null) {
            throw new RuntimeException("brokerList must contain at least " +
                    "one Kafka broker");
        }
        kafkaProps.put(KafkaSinkConstants.BROKER_LIST_KEY, brokerList);
        String requiredKey = conf.getProperty(
                KafkaSinkConstants.REQUIRED_ACKS_CONF_KEY);
        if (requiredKey != null ) {
            kafkaProps.put(KafkaSinkConstants.REQUIRED_ACKS_KEY, requiredKey);
        }
    }

}
