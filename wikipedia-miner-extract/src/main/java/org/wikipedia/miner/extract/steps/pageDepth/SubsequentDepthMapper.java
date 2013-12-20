package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;

public class SubsequentDepthMapper extends AvroMapper<Pair<Integer, PageDepthSummary>, Pair<Integer, PageDepthSummary>> {


	@Override
	public void map(Pair<Integer, PageDepthSummary> pair,
			AvroCollector<Pair<Integer, PageDepthSummary>> collector,
			Reporter reporter) throws IOException {
		
	
		Integer id = pair.key() ;
		PageDepthSummary depthSummary = pair.value() ;
		
		
		if (depthSummary.getDepthForwarded()) {
			//if we have already processed this in previous iterations, just pass it along directly
			InitialDepthMapper.collect(id, depthSummary, collector);
			return ;
		}
		
		if (depthSummary.getDepth() == null) { 
			//if we haven't reached this node yet, just pass it along directly
			InitialDepthMapper.collect(id, depthSummary, collector);
			return ;
		}
	
		InitialDepthMapper.shareDepth(depthSummary, collector, reporter) ;
		InitialDepthMapper.collect(id, depthSummary, collector);		
	}
	
}
