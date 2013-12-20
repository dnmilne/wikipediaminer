package org.wikipedia.miner.extract.steps.pageSummary;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.DumpLink;
import org.wikipedia.miner.extract.model.DumpLinkParser;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.steps.pageSummary.PageSummaryStep.SummaryPageType;
import org.wikipedia.miner.extract.util.Languages;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.PageSentenceExtractor;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.model.Page.PageType;
import org.wikipedia.miner.util.MarkupStripper;


public class InitialMapper extends MapReduceBase implements Mapper<LongWritable, Text, AvroKey<PageKey>, AvroValue<PageDetail>> {

	private static Logger logger = Logger.getLogger(InitialMapper.class) ;

	private Language language ;
	private SiteInfo siteInfo ;

	private DumpPageParser pageParser ;
	private DumpLinkParser linkParser ;

	private MarkupStripper stripper = new MarkupStripper() ;
	private PageSentenceExtractor sentenceExtractor ;

	
	private String[] debugTitles = {"Atheist","Atheism","Atheists","Athiest","People by religion"} ;



	@Override
	public void configure(JobConf job) {

		try {

			language = null ;
			siteInfo = null ;

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

			for (Path cf:cacheFiles) {

				if (cf.getName().equals(new Path(DumpExtractor.OUTPUT_SITEINFO).getName())) {
					siteInfo = SiteInfo.load(new File(cf.toString())) ;
				}

				if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
					language = Languages.load(new File(cf.toString())).get(job.get(DumpExtractor.KEY_LANG_CODE)) ;
				}

				if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_SENTENCE_MODEL)).getName())) {
					sentenceExtractor = new PageSentenceExtractor(cf) ;
				}
			}

			if (siteInfo == null) 
				throw new Exception("Could not locate '" + DumpExtractor.OUTPUT_SITEINFO + "' in DistributedCache") ;

			if (language == null) 
				throw new Exception("Could not locate '" + job.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

			pageParser = new DumpPageParser(language, siteInfo) ;
			linkParser = new DumpLinkParser(language, siteInfo) ;

			//rootCategoryTitle = Util.normaliseTitle(languageConfig.getRootCategoryName()) ;

		} catch (Exception e) {

			logger.error("Could not configure mapper", e);
		}
	}







	public void map(LongWritable key, Text value, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

		DumpPage parsedPage = null ;

		try {
			parsedPage = pageParser.parsePage(value.toString()) ;
		} catch (Exception e) {
			reporter.incrCounter(SummaryPageType.unparseable, 1);
			logger.error("Could not parse dump page " , e) ;
		}

		if (parsedPage == null)
			return ;

		switch (parsedPage.getType()) {

		case article :
			reporter.incrCounter(SummaryPageType.article, 1);
			handleArticleOrCategory(parsedPage, collector, reporter) ;

			break ;
		case category :
			reporter.incrCounter(SummaryPageType.category, 1);
			handleArticleOrCategory(parsedPage, collector, reporter) ;

			break ;
		case disambiguation :
			reporter.incrCounter(SummaryPageType.disambiguation, 1);

			//apart from the counting, don't treat disambig pages any different from ordinary articles
			handleArticleOrCategory(parsedPage, collector, reporter) ;

			break ;
		case redirect :
			if (parsedPage.getNamespace().getKey() == SiteInfo.MAIN_KEY)
				reporter.incrCounter(SummaryPageType.articleRedirect, 1);

			if (parsedPage.getNamespace().getKey() == SiteInfo.CATEGORY_KEY)
				reporter.incrCounter(SummaryPageType.categoryRedirect, 1);

			handleRedirect(parsedPage, collector, reporter) ;

			break ;
		default:
			//for all other page types (e.g. templates) do nothing
			return ;
		}

	}

	private PageKey buildKey(DumpPage parsedPage) {
		
		PageKey key = new PageKey() ;
		
		key.setNamespace(parsedPage.getNamespace().getKey());
		key.setTitle(parsedPage.getTitle());
		
		return key ;
	}


	private PageDetail buildBasePageDetails(DumpPage parsedPage) {


		PageDetail page = PageSummaryStep.buildEmptyPageDetail() ;
		
		page.setId(parsedPage.getId());
		
		if (parsedPage.getType().equals(PageType.disambiguation))
			page.setIsDisambiguation(true);
		
		//note: we don't set namespace or title, because these will be found in page keys (so it would be wasteful to repeat them)

		if (parsedPage.getTarget() != null)
			page.setRedirectsTo(new PageSummary(-1,parsedPage.getTarget(), parsedPage.getNamespace().getKey(), false));

		if (parsedPage.getLastEdited() != null)
			page.setLastEdited(parsedPage.getLastEdited().getTime());
		

		return page ;
	}
	
	private PageSummary buildPageSummary(DumpPage parsedPage) {
		
		PageSummary summary = new PageSummary() ;
		summary.setId(parsedPage.getId());
		summary.setNamespace(parsedPage.getNamespace().getKey());
		summary.setTitle(parsedPage.getTitle());
		
		
		return summary ;
	}

	/*
	private void handleCategory(DumpPage parsedPage, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

		PageDetail page = buildBasePageDetails(parsedPage) ;
		collect(new PageKey(page.getNamespace(), page.getTitle()), page, collector) ;

		if (page.getTitle().equals(rootCategoryTitle)) {
			logger.info("Root category id: " + parsedPage.getId()) ;
		}

		handleLinks(page,  parsedPage.getMarkup(), collector, reporter) ;
	}*/

	private void handleArticleOrCategory(DumpPage parsedPage, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

		boolean debug = false ;
		for(String debugTitle:debugTitles) {
			if (parsedPage.getTitle().equalsIgnoreCase(debugTitle))
				debug = true ;
		}
		
		PageKey key = buildKey(parsedPage) ;
		PageDetail page = buildBasePageDetails(parsedPage) ;
		
		try {
			List<Integer> sentenceSplits = sentenceExtractor.getSentenceSplits(parsedPage) ;	
			page.setSentenceSplits(sentenceSplits);
		} catch (Exception e) {
			logger.warn("Could not gather sentence splits for " + parsedPage.getTitle(), e) ;	
			logger.info(parsedPage.getMarkup());
		}
	
		collect(key, page, collector) ;

		handleLinks(key, page,  parsedPage.getMarkup(), collector, reporter) ;

		if (debug)
			logger.info(page);
		
		
	}

	


	private void handleRedirect(DumpPage parsedPage, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {


		PageDetail page = buildBasePageDetails(parsedPage) ;
		collect(buildKey(parsedPage), page, collector) ;

		String targetTitle = parsedPage.getTarget() ;

		// emit a pair to associate this redirect with target

		PageSummary source = buildPageSummary(parsedPage) ;
		source.setForwarded(false) ;

		PageDetail target = PageSummaryStep.buildEmptyPageDetail() ;
		target.getRedirects().add(source);

		collect(new PageKey(parsedPage.getNamespace().getKey(), targetTitle), target, collector) ;
	}


	public void handleLinks(PageKey key, PageDetail page, String markup, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

		String strippedMarkup = null ;

		try {

			strippedMarkup = stripper.stripAllButInternalLinksAndEmphasis(markup, ' ') ;

		} catch (Exception e) {

			logger.warn("Could not process link markup for " + page.getId() + ":" + key.getTitle());
			return ;
		}

		Vector<int[]> linkRegions = stripper.gatherComplexRegions(strippedMarkup, "\\[\\[", "\\]\\]") ;

		Map<String,PageDetail> linkTargets = new HashMap<String,PageDetail>() ;
		Map<String,PageDetail> categoryParents = new HashMap<String,PageDetail>() ;
		
		for(int[] linkRegion: linkRegions) {
			
			
			String linkMarkup = strippedMarkup.substring(linkRegion[0]+2, linkRegion[1]-2) ;

			DumpLink link = null ;
			try {
				link = linkParser.parseLink(linkMarkup, key.getTitle().toString()) ;
			} catch (Exception e) {
				logger.warn("Could not parse link markup '" + linkMarkup + "'") ;
			}

			if (link == null)
				continue ;

			if (link.getTargetLanguage() != null) {
				//logger.info("Language link: " + linkMarkup);
				
				//TODO: how do we get translations now?
				continue ;
			}

			if (link.getTargetNamespace().getKey()==SiteInfo.CATEGORY_KEY) {
				String parentTitle = link.getTargetTitle() ;
				PageDetail parent = buildCategoryParent(key, page, link) ;
				
				if (parent != null)
					categoryParents.put(parentTitle, parent) ;	
			}

			if (link.getTargetNamespace().getKey()==SiteInfo.MAIN_KEY) {
				String targetTitle = link.getTargetTitle() ;
				
				PageDetail target = linkTargets.get(targetTitle) ;
				if (target == null)
					target = PageSummaryStep.buildEmptyPageDetail() ;
				
				target = buildLinkTarget(key, page, link, linkRegion[0], target) ;

				linkTargets.put(targetTitle, target) ;
				
				if (link.getAnchor().contains("|"))
					logger.warn("weird link in " + key.getTitle() + ": \"" + linkMarkup + "\"");
				
			}
				

			
		}
		
		//emit collected link targets
		for (Map.Entry<String,PageDetail> e:linkTargets.entrySet()) {
			PageKey targetKey = new PageKey(SiteInfo.MAIN_KEY, e.getKey()) ;
			collect(targetKey, e.getValue(), collector) ;
		}
		
		//emit collected category parents
		for (Map.Entry<String,PageDetail> e:categoryParents.entrySet()) {
			PageKey parentKey = new PageKey(SiteInfo.CATEGORY_KEY, e.getKey()) ;
			collect(parentKey, e.getValue(), collector) ;
		}
		
		
	}

	



	private PageDetail buildLinkTarget(PageKey currKey, PageDetail currPage, DumpLink link, int linkStart, PageDetail targetPage) {

		/*
		emit details of this link, so it can be picked up by target
		the details we want to emit are 
			* that the current page is the source of the link,
			* the sentence index within the current page that this link is found
			* the link anchor text used for the link (i.e. the label)
		 */
		
		//basics about the link source
		
		LinkSummary source ;
		if (targetPage.getLinksIn().isEmpty()) {
		
			source = new LinkSummary() ;
			source.setId(currPage.getId());
			source.setTitle(currKey.getTitle());
			source.setNamespace(currKey.getNamespace()) ;
			source.setForwarded(false) ;
			source.setSentenceIndexes(new ArrayList<Integer>());
		
			targetPage.getLinksIn().add(source) ;
		} else {
			source = targetPage.getLinksIn().get(0) ;
		}
		
		//sentence index of the link
		int sentenceIndex = Collections.binarySearch(currPage.getSentenceSplits(), linkStart) ;
		if (sentenceIndex < 0)
			sentenceIndex = ((1-sentenceIndex) - 1) ;
		
		source.getSentenceIndexes().add(sentenceIndex) ;
		
		//the anchor text of the link
		LabelSummary label = targetPage.getLabels().get(link.getAnchor()) ;
		
		if (label == null) {
			label = new LabelSummary() ;
			label.setDocCount(1) ;
			label.setOccCount(1) ;	
			
			targetPage.getLabels().put(link.getAnchor(), label) ;
		} else {
			label.setOccCount(label.getOccCount() + 1);
		}
		
		//associate everything with target of link
		//PageDetail target = PageSummaryStep.buildEmptyPageDetail() ;
		//target.getLinksIn().add(source) ;
		//target.getLabels().add(label) ;

		return targetPage ;
	}

	/**
	 * This will emit a pair that will associate this current page as the child of the target category.
	 * The link will need to be backtracked before we can register the target as a parent of the source. 
	 * It may also need to be forwarded via any redirects 
	 * 
	 * @param currPage
	 * @param link
	 * @throws IOException 
	 */
	private PageDetail buildCategoryParent(PageKey currKey, PageDetail currPage, DumpLink link) {

		//emit details of this link, so it can be picked up by target
		PageSummary child = new PageSummary() ;
		child.setId(currPage.getId());
		child.setTitle(currKey.getTitle());
		child.setNamespace(currKey.getNamespace()) ;
		child.setForwarded(false) ; 


		PageDetail parent = PageSummaryStep.buildEmptyPageDetail() ;

		if (currKey.getNamespace() == SiteInfo.CATEGORY_KEY) 
			parent.getChildCategories().add(child);
		else if (currKey.getNamespace() == SiteInfo.MAIN_KEY)
			parent.getChildArticles().add(child);
		else
			return null ;

		return parent ;
	}


	

	private void collect(PageKey key, PageDetail value, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector) throws IOException {

		AvroKey<PageKey> k = new AvroKey<PageKey>(key) ;
		AvroValue<PageDetail> v = new AvroValue<PageDetail>(value) ;

		collector.collect(k,v) ;
	}
}
