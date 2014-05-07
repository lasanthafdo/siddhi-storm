package org.wso2.siddhi.storm.components;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class EchoBolt extends BaseBasicBolt {
    private AtomicInteger count = new AtomicInteger();
    private static final int BATCH_SIZE = 100000;
    private long lastTs = System.currentTimeMillis();
    private Log log = LogFactory.getLog(EchoBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        int localCount = count.incrementAndGet();
        if (log.isDebugEnabled()) {
            log.debug("[" + localCount + "] Result: " + tuple.getSourceStreamId() + " " + tuple.getValues());
        }
        if (localCount % BATCH_SIZE == 0) {
            log.info("[" + localCount + "] Throughput: " + (BATCH_SIZE * 1000 / (System.currentTimeMillis() - lastTs)));
            lastTs = System.currentTimeMillis();
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}
