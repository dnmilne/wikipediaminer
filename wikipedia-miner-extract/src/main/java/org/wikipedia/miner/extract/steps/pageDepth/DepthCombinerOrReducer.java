package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.wikipedia.miner.extract.model.struct.PageDepthSummary;

public abstract class DepthCombinerOrReducer extends AvroReducer<Integer, PageDepthSummary, Pair<Integer, PageDepthSummary>> {

	public enum Counts {unforwarded, withDepth,withoutDepth} ;
	
	
	public abstract boolean isReducer() ;
	

	@Override
	public void reduce(Integer pageId, Iterable<PageDepthSummary> partials,
			AvroCollector<Pair<Integer, PageDepthSummary>> collector,
			Reporter reporter) throws IOException {
		
		Integer minDepth = null ;
		boolean depthForwarded = false ;
		
		List<Integer> childIds = new ArrayList<Integer>();
		
		
		for (PageDepthSummary partial:partials) {
				
			if (partial.getDepth() != null) {
				if (minDepth == null || minDepth > partial.getDepth())  {
					minDepth = partial.getDepth().intValue() ;
					depthForwarded = partial.getDepthForwarded() ;
				}
			}
			
			if (!partial.getChildIds().isEmpty())
				childIds.addAll(partial.getChildIds()) ;
		}
		
		
		
		
		//if we haven't reached this node yet, just pass on as it is
		if (minDepth == null) {
			
			if (isReducer())
				reporter.getCounter(Counts.withoutDepth).increment(1);
			
			InitialDepthMapper.collect(pageId, new PageDepthSummary(minDepth, depthForwarded, childIds), collector);
			return ;
		}
	
		if (isReducer() ) {
			
			//depth forwarding is only required for pages with children
			if (childIds.isEmpty())
				depthForwarded = true ;
			
			//if we have already forwarded all details to children, then we don't need to keep track of them any more
			if (depthForwarded)
				childIds = new ArrayList<Integer>() ;
			
			//count stuff
			reporter.getCounter(Counts.withDepth).increment(1);
				
			if (!depthForwarded) 
				reporter.getCounter(Counts.unforwarded).increment(1);		
		}
		
		InitialDepthMapper.collect(pageId, new PageDepthSummary(minDepth, depthForwarded, childIds), collector);	
		
	}
	
	public static class DepthCombiner extends DepthCombinerOrReducer {

		@Override
		public boolean isReducer() {
			return false;
		}

	}

	public static class DepthReducer extends DepthCombinerOrReducer {

		@Override
		public boolean isReducer() {
			return true;
		}

	}
	
	
}
