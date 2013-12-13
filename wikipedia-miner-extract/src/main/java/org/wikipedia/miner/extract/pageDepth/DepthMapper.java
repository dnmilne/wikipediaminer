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
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.pageSummary.PageSummaryStep;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;

public class DepthMapper extends AvroMapper<Pair<PageKey, PageDetail>, Pair<PageKey, PageDetail>> {

	private static Logger logger = Logger.getLogger(DepthMapper.class) ;
	
	private String rootCategoryTitle ;
	
	
	@Override
	public void configure(JobConf job) {

		try {

			LanguageConfiguration languageConfig = null ;

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

			for (Path cf:cacheFiles) {

				if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
					languageConfig = new LanguageConfiguration(job.get(DumpExtractor.KEY_LANG_CODE), cf) ;
				}

			}

			if (languageConfig == null) 
				throw new Exception("Could not locate '" + job.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

			rootCategoryTitle = Util.normaliseTitle(languageConfig.getRootCategoryName()) ;

			
			
		} catch (Exception e) {

			logger.error("Could not configure mapper", e);
		}
		
		logger.info(rootCategoryTitle) ;
	}
	
	
	@Override
	public void map(Pair<PageKey, PageDetail> pair,
			AvroCollector<Pair<PageKey, PageDetail>> collector,
			Reporter reporter) throws IOException {
		
		if (rootCategoryTitle == null)
			throw new IOException("Mapper not configured with root category title") ;
		
		PageKey pageKey = pair.key() ;
		PageDetail page = pair.value() ;
		
		if (!pageKey.getNamespace().equals(SiteInfo.CATEGORY_KEY)) {
			//this only effects categories, just pass other page types along directly
			collect(pageKey, page, collector);
			return ;
		}
		
		if (page.getDepthForwarded()) {
			//if we have already processed this in previous iterations, just pass it along directly
			collect(pageKey, page, collector);
			return ;
		}
		
	
		if (page.getDepth() != null) {
			shareDepth(page, collector, reporter) ;
		} else if (rootCategoryTitle.equals(pageKey.getTitle().toString())) {
				
			page.setDepth(0) ;
			shareDepth(page, collector, reporter) ;
		} 
		
		collect(pageKey, page, collector);		
	}
	
	private void shareDepth(PageDetail page, AvroCollector<Pair<PageKey, PageDetail>> collector, Reporter reporter) throws IOException {
		
		if (page.getDepth() == null)
			return ;
		
		if (page.getDepthForwarded())
			return ;
		
		if (page.getNamespace() != SiteInfo.CATEGORY_KEY)
			return ;
		
		logger.info("sharing depths for " + page.getTitle() + ": " + page.getDepth());
		
		for (PageSummary cc: page.getChildCategories()) {
			
			PageDetail child = PageSummaryStep.buildEmptyPageDetail() ;
			child.setDepth(page.getDepth() + 1);
			child.setDepthForwarded(false);
			
			PageKey childKey = new PageKey(SiteInfo.CATEGORY_KEY, cc.getTitle()) ;
			collect(childKey, child, collector) ;
		}
		
		for (PageSummary ca: page.getChildArticles()) {
			
			PageDetail child = PageSummaryStep.buildEmptyPageDetail() ;
			child.setDepth(page.getDepth() + 1);
			child.setDepthForwarded(false);
			
			PageKey childKey = new PageKey(SiteInfo.MAIN_KEY, ca.getTitle()) ;
			collect(childKey, child, collector) ;
		}
				
		page.setDepthForwarded(true);
	}
	
	private void collect(PageKey key, PageDetail page, AvroCollector<Pair<PageKey, PageDetail>> collector) throws IOException {
		collector.collect(new Pair<PageKey,PageDetail>(key,page));
	}
		
	
}
