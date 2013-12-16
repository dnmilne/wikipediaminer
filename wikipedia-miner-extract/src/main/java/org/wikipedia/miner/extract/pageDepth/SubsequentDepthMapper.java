package org.wikipedia.miner.extract.pageDepth;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.pageSummary.InitialMapper;
import org.wikipedia.miner.extract.pageSummary.PageSummaryStep;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;

public class SubsequentDepthMapper extends AvroMapper<Pair<Integer, PageDepthSummary>, Pair<Integer, PageDepthSummary>> {

	private static Logger logger = Logger.getLogger(SubsequentDepthMapper.class) ;
	
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
