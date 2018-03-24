package com.woophee.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class DataSpout implements IRichSpout {

    private SpoutOutputCollector spoutOutputCollector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector=spoutOutputCollector;
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        String sentence = "Hello";
        double msgId = Math.random();
        spoutOutputCollector.emit(new Values(sentence),msgId);
    }


    //必须emit制定msgId，ack和fail方法才会被回调
    public void ack(Object msgId) {
        System.out.println("ack:" + msgId);
    }

    public void fail(Object msgId) {
        System.out.println("fail:" + msgId);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
