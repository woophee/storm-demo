package com.woophee;

import com.woophee.bolt.AnalysisBolt;
import com.woophee.spout.DataSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

public class StormApplication {

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        TopologyBuilder analysisBuilder = analysisBuilder();
        Config analysisConfig = new Config();
        if (args != null && args.length > 0) {
            analysisConfig.put(Config.NIMBUS_HOST, args[0]);
            analysisConfig.setNumWorkers(10);
            StormSubmitter.submitTopologyWithProgressBar("analysis-topology", analysisConfig, analysisBuilder.createTopology());
        } else {
            analysisConfig.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("analysis-topology", analysisConfig, analysisBuilder.createTopology());
        }
    }

    private static TopologyBuilder analysisBuilder() {
        TopologyBuilder analysisBuilder = new TopologyBuilder();
        analysisBuilder.setSpout("data-spout", new DataSpout(), 1);
        analysisBuilder.setBolt("analysis-bolt", new AnalysisBolt(),10).shuffleGrouping("data-spout");

        return analysisBuilder;
    }

}
