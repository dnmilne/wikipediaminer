package org.wikipedia.miner.extract.pageDepth;

import java.io.IOException;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.pageSummary.PageSummaryStep;
import org.wikipedia.miner.extract.util.SiteInfo;

public abstract class DepthCombinerOrReducer extends AvroReducer<PageKey, PageDetail, Pair<PageKey, PageDetail>> {

	public enum Counts {unforwarded, categoriesWithDepth, categoriesWithoutDepth, articlesWithDepth, articlesWithoutDepth} ;
	
	private static Logger logger = Logger.getLogger(DepthCombinerOrReducer.class) ;

	public abstract boolean isReducer() ;
	

	@Override
	public void reduce(PageKey key, Iterable<PageDetail> pagePartials,
			AvroCollector<Pair<PageKey, PageDetail>> collector,
			Reporter reporter) throws IOException {
		
		Integer minDepth = null ;
		boolean depthForwarded = false ;
		
		PageDetail pageDetail = null ;
		
		
		for (PageDetail pagePartial:pagePartials) {
			
			//if we know the id, then this partial must have the rest of the detail
			
			if (pagePartial.getId() != null)
				pageDetail = PageSummaryStep.clone(pagePartial) ;
			
			if (pagePartial.getDepth() != null) {
				if (minDepth == null || minDepth > pagePartial.getDepth())  {
					minDepth = pagePartial.getDepth().intValue() ;
					depthForwarded = pagePartial.getDepthForwarded() ;
				}
			}

		}
		
		if (pageDetail == null) {
			if (isReducer())
				throw new IOException("Could not retrieve full details of " + key);
			else
				pageDetail = PageSummaryStep.buildEmptyPageDetail() ;			
		}
		
		if (pageDetail.getRedirectsTo() != null || (key.getNamespace() != SiteInfo.CATEGORY_KEY && key.getNamespace() != SiteInfo.MAIN_KEY)) {
			
			//if this is a redirect or neither article nor category, just pass directly along
			collector.collect(new Pair<PageKey,PageDetail>(key,pageDetail));	
			return ;
		}
		

		//depth forwarding is only required for categories
		if (key.getNamespace() != SiteInfo.CATEGORY_KEY)
			depthForwarded = true ;
		
		
		//count stuff
		if (isReducer() ) {
		
			if (minDepth == null) {
				
				if (key.getNamespace() == SiteInfo.CATEGORY_KEY)
					reporter.getCounter(Counts.categoriesWithoutDepth).increment(1);
				else
					reporter.getCounter(Counts.articlesWithoutDepth).increment(1);
				
			} else {
				
				if (!depthForwarded) 
					reporter.getCounter(Counts.unforwarded).increment(1);
				
				if (key.getNamespace() == SiteInfo.CATEGORY_KEY)
					reporter.getCounter(Counts.categoriesWithDepth).increment(1);
				else
					reporter.getCounter(Counts.articlesWithDepth).increment(1);
			}			
		}
		
		pageDetail.setDepth(minDepth);
		pageDetail.setDepthForwarded(depthForwarded);
			
		collector.collect(new Pair<PageKey,PageDetail>(key,pageDetail));	
		
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
