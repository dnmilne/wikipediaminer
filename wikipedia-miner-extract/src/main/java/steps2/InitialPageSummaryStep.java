package steps2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.DumpLink;
import org.wikipedia.miner.extract.model.DumpLinkParser;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.extract.util.XmlInputFormat;
import org.wikipedia.miner.util.MarkupStripper;



public class InitialPageSummaryStep extends PageSummaryStep {
	
	private static Logger logger = Logger.getLogger(InitialPageSummaryStep.class) ;
	
	public InitialPageSummaryStep(Path baseWorkingDir) throws IOException {

		super(baseWorkingDir) ;
	}


	public int run(String[] args) throws UncompletedStepException, IOException {

		if (isFinished())
			return 0 ;
		else
			reset() ;

		JobConf conf = new JobConf(InitialPageSummaryStep.class);
		DumpExtractor.configureJob(conf, args) ;

		conf.setJobName("WM: initial page summary step");

		conf.setOutputKeyClass(AvroKey.class);
		conf.setOutputValueClass(AvroValue.class);

		conf.setMapperClass(Map.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;

		AvroJob.setReducerClass(conf, Reduce.class);
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(PageKey.getClassSchema(),PageDetail.getClassSchema()));
		
		
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);

		FileInputFormat.setInputPaths(conf, conf.get(DumpExtractor.KEY_INPUT_FILE));
		FileOutputFormat.setOutputPath(conf, getWorkingDir());

		RunningJob runningJob = JobClient.runJob(conf);

		finish(runningJob) ;
		
		if (runningJob.getJobState() == JobStatus.SUCCEEDED)
			return 0 ;
		
		throw new UncompletedStepException() ;
		
	}

	public String getWorkingDirName() {
		return "page_0" ;
	}

	private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, AvroKey<PageKey>, AvroValue<PageDetail>> {

		private LanguageConfiguration lc ;
		
		private DumpPageParser pageParser ;
		private DumpLinkParser linkParser ;
		
		private MarkupStripper stripper = new MarkupStripper() ;

		private String rootCategoryTitle ;

		@Override
		public void configure(JobConf job) {

			try {

				lc = null ;
				SiteInfo si = null ;

				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

				for (Path cf:cacheFiles) {

					if (cf.getName().equals(new Path(DumpExtractor.OUTPUT_SITEINFO).getName())) {
						si = new SiteInfo(cf) ;
					}

					if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
						lc = new LanguageConfiguration(job.get(DumpExtractor.KEY_LANG_CODE), cf) ;
					}
				}

				if (si == null) 
					throw new Exception("Could not locate '" + DumpExtractor.OUTPUT_SITEINFO + "' in DistributedCache") ;

				if (lc == null) 
					throw new Exception("Could not locate '" + job.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;
				
				pageParser = new DumpPageParser(lc, si) ;
				linkParser = new DumpLinkParser(lc, si) ;
				
				rootCategoryTitle = Util.normaliseTitle(lc.getRootCategoryName()) ;

			} catch (Exception e) {


				Logger.getLogger(Map.class).error("Could not configure mapper", e);
			}
		}



		
		


		public void map(LongWritable key, Text value, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

			DumpPage parsedPage = null ;

			try {
				parsedPage = pageParser.parsePage(value.toString()) ;
			} catch (Exception e) {
				reporter.incrCounter(PageTypeCount.unparseable, 1);
				logger.error("Could not parse dump page " , e) ;
			}

			if (parsedPage == null)
				return ;

			
			

			
			
			
			//page.setIsRedirect(parsedPage.getType() == PageType.redirect);

			

			switch (parsedPage.getType()) {

			case article :
				reporter.incrCounter(PageTypeCount.article, 1);
				handleArticle(parsedPage, collector, reporter) ;
				
				break ;
			case category :
				reporter.incrCounter(PageTypeCount.category, 1);
				handleCategory(parsedPage, collector, reporter) ;
				
				break ;
			case disambiguation :
				reporter.incrCounter(PageTypeCount.disambiguation, 1);
				
				//apart from the counting, don't treat disambig pages any different from ordinary articles
				handleArticle(parsedPage, collector, reporter) ;
				
				break ;
			case redirect :
				reporter.incrCounter(PageTypeCount.redirect, 1);
				handleRedirect(parsedPage, collector, reporter) ;
				
				break ;
			default:
				//for all other page types (e.g. templates) do nothing
				return ;
			}

		}
		
		
		
		
		private PageDetail buildBasePageDetails(DumpPage parsedPage) {
			
			
			PageDetail page = buildEmptyPageDetail() ;
			
						
			page.setNamespace(parsedPage.getNamespace());
			page.setId(parsedPage.getId());
			page.setTitle(Util.normaliseTitle(parsedPage.getTitle())) ;
			
			if (parsedPage.getTarget() != null)
				page.setRedirectsTo(new PageSummary(-1,Util.normaliseTitle(parsedPage.getTarget()), page.getNamespace(), false));
			
			if (parsedPage.getLastEdited() != null)
				page.setLastEdited(parsedPage.getLastEdited().getTime());
			
			return page ;
		}
		
		private void handleCategory(DumpPage parsedPage, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {
			
			PageDetail page = buildBasePageDetails(parsedPage) ;
			collect(new PageKey(page.getNamespace(), page.getTitle()), page, collector) ;
			
			if (page.getTitle().equals(rootCategoryTitle)) {
				//reporter.incrCounter(PageTypeCount.rootCategoryCount, 1);
				reporter.incrCounter(PageTypeCount.rootCategoryId, parsedPage.getId()) ;
			}
			
			handleLinks(page,  parsedPage.getMarkup(), collector, reporter) ;
		}
		
		private void handleArticle(DumpPage parsedPage, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

			PageDetail page = buildBasePageDetails(parsedPage) ;
			collect(new PageKey(page.getNamespace(), page.getTitle()), page, collector) ;
			
			handleLinks(page,  parsedPage.getMarkup(), collector, reporter) ;

		}
		
		private void handleRedirect(DumpPage parsedPage, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

		
			PageDetail page = buildBasePageDetails(parsedPage) ;
			collect(new PageKey(page.getNamespace(), page.getTitle()), page, collector) ;
			
			String targetTitle = Util.normaliseTitle(parsedPage.getTarget()) ;

			// emit a pair to associate this redirect with target
			
			PageSummary source = new PageSummary() ;
			source.setId(page.getId());
			source.setTitle(page.getTitle());
			source.setForwarded(false) ;
			
			PageDetail target = buildEmptyPageDetail() ;
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
					handleArticleLink(page, link, collector) ;

				
				
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
		private void handleArticleLink(PageDetail currPage, DumpLink link, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector) throws IOException {
			
			//emit details of this link, so it can be picked up by target
			PageSummary source = new PageSummary() ;
			source.setId(currPage.getId());
			source.setTitle(currPage.getTitle());
			source.setForwarded(false) ;
			
			PageDetail target = buildEmptyPageDetail() ;
			target.getLinksIn().add(source) ;
 			
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
			child.setForwarded(false) ; 
			
			
			PageDetail parent = buildEmptyPageDetail() ;
			
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
	
	


}
