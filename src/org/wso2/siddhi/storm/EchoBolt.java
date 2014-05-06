package org.wso2.siddhi.storm;
import java.util.concurrent.atomic.AtomicInteger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class EchoBolt extends BaseBasicBolt {
	AtomicInteger count = new AtomicInteger();
	long lastTs = System.currentTimeMillis();
	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {
		int temp = count.incrementAndGet(); 
		//if(temp%1000 == 0){
			System.out.println("[" + temp  +" ]Result" + tuple.getSourceStreamId() + " "+ tuple.getValues());	
		//}
		if(temp%10000 == 0){
			System.out.println("["+temp +"]Throughput=" + (10000*1000/(System.currentTimeMillis() - lastTs)));
			lastTs = System.currentTimeMillis();
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}
