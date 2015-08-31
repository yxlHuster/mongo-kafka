package com.zy.bi.main;

import com.zy.bi.sinks.KafkaSink;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

/**
 * Created by allen on 2015/8/31.
 */
public class StartUp {


    public static void main(String[] args) {
        Options opts = new Options();
        opts.addOption("h", "help", false, "usage: -c config");
        opts.addOption("c", "conf", true, "config file");
        BasicParser parser = new BasicParser();
        try {
            CommandLine cl = parser.parse(opts, args);
            HelpFormatter formatter = new HelpFormatter();
            if (cl.getOptions().length > 0) {
                if (cl.hasOption("h")) {
                    formatter.printHelp("Options", opts);
                } else {
                    String conf = cl.getOptionValue("c");
                    KafkaSink kafkaSink = new KafkaSink();
                    kafkaSink.configure(conf);
                    kafkaSink.start();
                }
            } else {
                formatter.printHelp("Options", opts);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
