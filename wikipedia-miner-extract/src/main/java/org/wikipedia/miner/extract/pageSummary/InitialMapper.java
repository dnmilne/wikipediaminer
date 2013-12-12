package org.wikipedia.miner.extract.pageSummary;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.Span;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.commons.lang.StringUtils;
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
import org.wikipedia.miner.extract.model.struct.LabelCount;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.pageSummary.PageSummaryStep.PageType;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.PageSentenceExtractor;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.util.MarkupStripper;


public class InitialMapper extends MapReduceBase implements Mapper<LongWritable, Text, AvroKey<PageKey>, AvroValue<PageDetail>> {

	private static Logger logger = Logger.getLogger(InitialMapper.class) ;

	private LanguageConfiguration languageConfig ;
	private SiteInfo siteInfo ;

	private DumpPageParser pageParser ;
	private DumpLinkParser linkParser ;

	private MarkupStripper stripper = new MarkupStripper() ;
	private PageSentenceExtractor sentenceExtractor ;

	
	private String[] debugTitles = {"Atheist","Atheism","Atheists","Athiest","People by religion"} ;



	@Override
	public void configure(JobConf job) {

		try {

			languageConfig = null ;
			siteInfo = null ;

			Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

			for (Path cf:cacheFiles) {

				if (cf.getName().equals(new Path(DumpExtractor.OUTPUT_SITEINFO).getName())) {
					siteInfo = new SiteInfo(cf) ;
				}

				if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
					languageConfig = new LanguageConfiguration(job.get(DumpExtractor.KEY_LANG_CODE), cf) ;
				}

				if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_SENTENCE_MODEL)).getName())) {
					sentenceExtractor = new PageSentenceExtractor(cf) ;
				}
			}

			if (siteInfo == null) 
				throw new Exception("Could not locate '" + DumpExtractor.OUTPUT_SITEINFO + "' in DistributedCache") ;

			if (languageConfig == null) 
				throw new Exception("Could not locate '" + job.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

			pageParser = new DumpPageParser(languageConfig, siteInfo) ;
			linkParser = new DumpLinkParser(languageConfig, siteInfo) ;

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
			reporter.incrCounter(PageType.unparseable, 1);
			logger.error("Could not parse dump page " , e) ;
		}

		if (parsedPage == null)
			return ;

		switch (parsedPage.getType()) {

		case article :
			reporter.incrCounter(PageType.article, 1);
			handleArticleOrCategory(parsedPage, collector, reporter) ;

			break ;
		case category :
			reporter.incrCounter(PageType.category, 1);
			handleArticleOrCategory(parsedPage, collector, reporter) ;

			break ;
		case disambiguation :
			reporter.incrCounter(PageType.disambiguation, 1);

			//apart from the counting, don't treat disambig pages any different from ordinary articles
			handleArticleOrCategory(parsedPage, collector, reporter) ;

			break ;
		case redirect :
			if (parsedPage.getNamespace() == SiteInfo.MAIN_KEY)
				reporter.incrCounter(PageType.articleRedirect, 1);

			if (parsedPage.getNamespace() == SiteInfo.CATEGORY_KEY)
				reporter.incrCounter(PageType.categoryRedirect, 1);

			handleRedirect(parsedPage, collector, reporter) ;

			break ;
		default:
			//for all other page types (e.g. templates) do nothing
			return ;
		}

	}




	private PageDetail buildBasePageDetails(DumpPage parsedPage) {


		PageDetail page = PageSummaryStep.buildEmptyPageDetail() ;


		page.setNamespace(parsedPage.getNamespace());
		page.setId(parsedPage.getId());
		page.setTitle(Util.normaliseTitle(parsedPage.getTitle())) ;

		if (parsedPage.getTarget() != null)
			page.setRedirectsTo(new PageSummary(-1,Util.normaliseTitle(parsedPage.getTarget()), page.getNamespace(), false));

		if (parsedPage.getLastEdited() != null)
			page.setLastEdited(parsedPage.getLastEdited().getTime());
		
		

		return page ;
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
		
		PageDetail page = buildBasePageDetails(parsedPage) ;
		
		try {
			List<Integer> sentenceSplits = sentenceExtractor.getSentenceSplits(parsedPage) ;
			
			//logger.info("splits for " + parsedPage.getTitle() + ": [" + StringUtils.join(sentenceSplits, ",") + "]") ;
					
			page.setSentenceSplits(sentenceSplits);
		} catch (Exception e) {
			logger.warn("Could not gather sentence splits for " + page.getTitle(), e) ;	
			logger.info(parsedPage.getMarkup());
		}
	
		collect(new PageKey(page.getNamespace(), page.getTitle()), page, collector) ;

		handleLinks(page,  parsedPage.getMarkup(), collector, reporter) ;

		if (debug)
			logger.info(page);
		
		
	}

	


	private void handleRedirect(DumpPage parsedPage, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {


		PageDetail page = buildBasePageDetails(parsedPage) ;
		collect(new PageKey(page.getNamespace(), page.getTitle()), page, collector) ;

		String targetTitle = Util.normaliseTitle(parsedPage.getTarget()) ;

		// emit a pair to associate this redirect with target

		PageSummary source = new PageSummary() ;
		source.setId(page.getId());
		source.setTitle(page.getTitle());
		source.setNamespace(page.getNamespace()) ;
		source.setForwarded(false) ;

		PageDetail target = PageSummaryStep.buildEmptyPageDetail() ;
		target.getRedirects().add(source);

		collect(new PageKey(page.getNamespace(), targetTitle), target, collector) ;
	}


	public void handleLinks(PageDetail page, String markup, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

		String strippedMarkup = null ;

		try {

			strippedMarkup = stripper.stripAllButInternalLinksAndEmphasis(markup, ' ') ;

		} catch (Exception e) {

			logger.warn("Could not process link markup for " + page.getId() + ":" + page.getTitle());
			return ;
		}

		Vector<int[]> linkRegions = stripper.gatherComplexRegions(strippedMarkup, "\\[\\[", "\\]\\]") ;

		for(int[] linkRegion: linkRegions) {
			
			
			String linkMarkup = strippedMarkup.substring(linkRegion[0]+2, linkRegion[1]-2) ;

			DumpLink link = null ;
			try {
				link = linkParser.parseLink(linkMarkup) ;
			} catch (Exception e) {
				logger.warn("Could not parse link markup '" + linkMarkup + "'") ;
			}

			if (link == null)
				continue ;

			if (link.getTargetLanguage() != null) {
				logger.info("Language link: " + linkMarkup);
				continue ;
			}

			if (link.getTargetNamespace()==SiteInfo.CATEGORY_KEY) 
				handleCategoryLink(page, link, collector) ;

			if (link.getTargetNamespace()==SiteInfo.MAIN_KEY) 
				handleArticleLink(page, link, linkRegion[0], collector) ;



			//TODO: how do we get translations now?
		}			
	}






	/**
	 * This will emit a pair that will associate this current page as the source of an in-link to the target article.
	 * The link will need to be backtracked before we can register the target as a out link from the source. 
	 * It may also need to be forwarded via any redirects 
	 * 
	 * @param currPage
	 * @param link
	 * @param collector
	 * @throws IOException 
	 */
	private void handleArticleLink(PageDetail currPage, DumpLink link, int linkStart, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector) throws IOException {

		/*
		emit details of this link, so it can be picked up by target
		the details we want to emit are 
			* that the current page is the source of the link,
			* the sentence index within the current page that this link is found
			* the link anchor text used for the link (i.e. the label)
		 */
		
		//basics about the link source
		
		LinkSummary source = new LinkSummary() ;
		source.setId(currPage.getId());
		source.setTitle(currPage.getTitle());
		source.setNamespace(currPage.getNamespace()) ;
		source.setForwarded(false) ;
		
		
		//sentence index of the link
		
		int sentenceIndex = Collections.binarySearch(currPage.getSentenceSplits(), linkStart) ;
		if (sentenceIndex < 0)
			sentenceIndex = ((1-sentenceIndex) - 1) ;
		
		List<Integer> sentenceIndexes = new ArrayList<Integer>() ;
		sentenceIndexes.add(sentenceIndex) ;
		
		source.setSentenceIndexes(sentenceIndexes);
		
		
		//the anchor text of the link

		LabelCount labelCount = new LabelCount() ;
		labelCount.setLabel(link.getAnchor()) ;
		labelCount.setCount(1) ;

		
		
		
		
		
		
		//associate everything with target of link
		PageDetail target = PageSummaryStep.buildEmptyPageDetail() ;
		target.getLinksIn().add(source) ;
		target.getLabelCounts().add(labelCount) ;

		PageKey targetKey = new PageKey(link.getTargetNamespace(), Util.normaliseTitle(link.getTargetTitle())) ;

		collect(targetKey, target, collector) ;
	}

	/**
	 * This will emit a pair that will associate this current page as the child of the target category.
	 * The link will need to be backtracked before we can register the target as a parent of the source. 
	 * It may also need to be forwarded via any redirects 
	 * 
	 * @param currPage
	 * @param link
	 * @param collector
	 * @throws IOException 
	 */
	private void handleCategoryLink(PageDetail currPage, DumpLink link, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector) throws IOException {

		//emit details of this link, so it can be picked up by target
		PageSummary child = new PageSummary() ;
		child.setId(currPage.getId());
		child.setTitle(currPage.getTitle());
		child.setNamespace(currPage.getNamespace()) ;
		child.setForwarded(false) ; 


		PageDetail parent = PageSummaryStep.buildEmptyPageDetail() ;

		if (currPage.getNamespace() == SiteInfo.CATEGORY_KEY) 
			parent.getChildCategories().add(child);
		else if (currPage.getNamespace() == SiteInfo.MAIN_KEY)
			parent.getChildArticles().add(child);
		else
			return ;

		PageKey parentKey = new PageKey(link.getTargetNamespace(), Util.normaliseTitle(link.getTargetTitle())) ;

		collect(parentKey, parent, collector) ;
	}


	

	private void collect(PageKey key, PageDetail value, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector) throws IOException {

		AvroKey<PageKey> k = new AvroKey<PageKey>(key) ;
		AvroValue<PageDetail> v = new AvroValue<PageDetail>(value) ;

		collector.collect(k,v) ;
	}
}
