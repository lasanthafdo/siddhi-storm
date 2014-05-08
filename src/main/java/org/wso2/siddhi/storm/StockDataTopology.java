package org.wso2.siddhi.storm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import org.wso2.siddhi.storm.components.EchoBolt;
import org.wso2.siddhi.storm.components.SiddhiBolt;
import org.wso2.siddhi.storm.components.StockDataSpout;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class StockDataTopology {


    private static SiddhiBolt configureSiddhiBolt1() {
        return new SiddhiBolt(
                new String[]{"define stream StockData ( symbol string, price double, volume int);"},
                new String[]{"from StockData#window.time(1sec) select symbol, price, avg(volume) as avgV group by symbol insert into AvgVolume;"},
                new String[]{"AvgVolume"});
    }

    private static SiddhiBolt configureSiddhiBolt2() {
        return new SiddhiBolt(
                new String[]{"define stream AvgRunPlay ( sid string, v double);"},
                new String[]{"from AvgRunPlay[v>20] select sid, v as v insert into FastRunPlay;"},
                new String[]{"FastRunPlay"});
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("StockData", new StockDataSpout(), 1);
        builder.setBolt("AvgVolume", configureSiddhiBolt1(), 1).shuffleGrouping("StockData");
        builder.setBolt("LeafEcho", new EchoBolt(), 1).shuffleGrouping("AvgVolume");

        Config conf = new Config();

        conf.setNumWorkers(3);
        StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
}