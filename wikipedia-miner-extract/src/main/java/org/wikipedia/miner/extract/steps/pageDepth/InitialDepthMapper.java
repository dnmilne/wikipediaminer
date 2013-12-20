package org.wikipedia.miner.extract.steps.pageDepth;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

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
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.util.Languages;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;

public class InitialDepthMapper extends AvroMapper<Pair<Integer, PageDetail>, Pair<Integer, PageDepthSummary>> {

	private static Logger logger = Logger.getLogger(SubsequentDepthMapper.class) ;
	
	private String rootCategoryTitle ;
	
	
	@Override
	public void configure(JobConf job) {

		try {

			Language language = null ;

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

			for (Path cf:cacheFiles) {

				if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
					language = Languages.load(new File(cf.toString())).get(job.get(DumpExtractor.KEY_LANG_CODE)) ;
				}

			}

			if (language == null) 
				throw new Exception("Could not locate '" + job.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

			rootCategoryTitle = Util.normaliseTitle(language.getRootCategory()) ;

			
			
		} catch (Exception e) {

			logger.error("Could not configure mapper", e);
		}
		
		logger.info(rootCategoryTitle) ;
	}
	
	
	@Override
	public void map(Pair<Integer, PageDetail> pair,
			AvroCollector<Pair<Integer, PageDepthSummary>> collector,
			Reporter reporter) throws IOException {
		
		if (rootCategoryTitle == null)
			throw new IOException("Mapper not configured with root category title") ;
		
		PageDetail page = pair.value() ;
		
		if (!page.getNamespace().equals(SiteInfo.CATEGORY_KEY) && !page.getNamespace().equals(SiteInfo.MAIN_KEY)) {
			//this only effects articles and categories, just discard other page types
			return ;
		}
		
		if (page.getRedirectsTo() != null) {
			//this doesn't effect redirects, so just discard them
			return ;
		}
		
		PageDepthSummary depthSummary = new PageDepthSummary() ;
		depthSummary.setChildIds(new ArrayList<Integer>()) ;
		
		for (PageSummary childCat:page.getChildCategories()) 
			depthSummary.getChildIds().add(childCat.getId()) ;
		
		for (PageSummary childArt:page.getChildArticles())
			depthSummary.getChildIds().add(childArt.getId()) ;
		
		if (rootCategoryTitle.equals(page.getTitle().toString())) {
			depthSummary.setDepth(0) ;
			shareDepth(depthSummary, collector, reporter) ;
		} 
		
		collect(page.getId(), depthSummary, collector);		
	}
	
	public static void shareDepth(PageDepthSummary page, AvroCollector<Pair<Integer, PageDepthSummary>> collector, Reporter reporter) throws IOException {
		
		if (page.getDepth() == null)
			return ;
		
		if (page.getDepthForwarded())
			return ;
		
		//logger.info("sharing depths for " + page.getTitle() + ": " + page.getDepth());
		for (Integer childId:page.getChildIds()) {
			
			PageDepthSummary child = new PageDepthSummary() ;
			child.setDepth(page.getDepth() + 1);
			child.setDepthForwarded(false);
			child.setChildIds(new ArrayList<Integer>());
			
			collect(childId, child, collector) ;
		}
		
		page.setDepthForwarded(true);
	}
	
	public static void collect(Integer pageId, PageDepthSummary pageDepth, AvroCollector<Pair<Integer, PageDepthSummary>> collector) throws IOException {
		collector.collect(new Pair<Integer,PageDepthSummary>(pageId,pageDepth));
	}
		
	
}
