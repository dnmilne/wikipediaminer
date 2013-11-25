package steps2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.DumpExtractor.ExtractionStep;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.extract.util.XmlInputFormat;
import org.wikipedia.miner.model.Page.PageType;



/**
 * The first step in the extraction process.
 * 
 * This produces the following sequence files (in <i>&lt;ouput_dir&gt;/step1/</i>)
 * <ul>
 * <li><b>tempPage-xxxxx</b> - a csv file associating Integer id with DbPage.</li>
 * <li><b>tempRedirect-xxxxx</b> - a csv file associating Integer id with the title of a redirect target.</li>
 * </ul>
 */
public class InitialPageSummaryStep extends Step {

	public enum Counter {articleCount, categoryCount, disambiguationCount, redirectCount, rootCategoryId, rootCategoryCount, unparsablePageCount, unforwardedRedirectCount} ;

	public InitialPageSummaryStep(Path baseWorkingDir) throws IOException {

		super(baseWorkingDir) ;
	}


	public int run(String[] args) throws Exception {

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

		return runningJob.getJobState() ;
	}

	public String getWorkingDirName() {
		return "page_0" ;
	}

	private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, AvroKey<PageKey>, AvroValue<PageDetail>> {

		private LanguageConfiguration lc ;
		private DumpPageParser dpp ;

		private MultipleOutputs mos ;

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

				dpp = new DumpPageParser(lc, si) ;

				mos = new MultipleOutputs(job);


				rootCategoryTitle = Util.normaliseTitle(lc.getRootCategoryName()) ;

			} catch (Exception e) {


				Logger.getLogger(Map.class).error("Could not configure mapper", e);
			}
		}



		
		


		public void map(LongWritable key, Text value, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector, Reporter reporter) throws IOException {

			DumpPage parsedPage = null ;

			try {
				parsedPage = dpp.parsePage(value.toString()) ;
			} catch (Exception e) {
				reporter.incrCounter(Counter.unparsablePageCount, 1);
				Logger.getLogger(Map.class).error("Caught exception", e) ;
			}

			if (parsedPage == null)
				return ;


			String title = Util.normaliseTitle(parsedPage.getTitle()) ;

			PageDetail page = new PageDetail() ;
			page.setRedirects(new ArrayList<PageSummary>()) ;
			page.setNamespace(parsedPage.getNamespace());
			page.setId(parsedPage.getId());
			page.setTitle(title) ;
			page.setIsRedirect(parsedPage.getType() == PageType.redirect);

			if (parsedPage.getLastEdited() != null)
				page.setLastEdited(parsedPage.getLastEdited().getTime());

			switch (parsedPage.getType()) {

			case article :
				reporter.incrCounter(Counter.articleCount, 1);

				break ;
			case category :
				reporter.incrCounter(Counter.categoryCount, 1);

				if (title.equals(rootCategoryTitle)) {
					reporter.incrCounter(Counter.rootCategoryCount, 1);
					reporter.incrCounter(Counter.rootCategoryId, parsedPage.getId()) ;
				}

				break ;
			case disambiguation :
				reporter.incrCounter(Counter.disambiguationCount, 1);
				break ;
			case redirect :
				reporter.incrCounter(Counter.redirectCount, 1);
				//mos.getCollector(Output.tempRedirect.name(), reporter).collect(new IntWritable(dp.getId()), new Text(dp.getTarget()));

				String targetTitle = Util.normaliseTitle(parsedPage.getTarget()) ;

				page.setRedirectsToTitle(targetTitle) ;

				// emit a pair to associate this redirect with target
				PageDetail targetSummary = new PageDetail() ;

				PageSummary redirect = new PageSummary() ;
				redirect.setId(parsedPage.getId());
				redirect.setTitle(title);
				redirect.setForwarded(false) ;
				redirect.setBacktracked(false) ;

				List<PageSummary> redirects = new ArrayList<PageSummary>() ;
				redirects.add(redirect) ;
				targetSummary.setRedirects(redirects);

				collect(new PageKey(parsedPage.getNamespace(), targetTitle), targetSummary, collector) ;

				break ;
			default:
				//for all other page types, do nothing
				return ;
			}

			collect(new PageKey(parsedPage.getNamespace(), title),  page, collector) ; 
		}
		
		private void collect(PageKey key, PageDetail value, OutputCollector<AvroKey<PageKey>, AvroValue<PageDetail>> collector) throws IOException {
			
			AvroKey<PageKey> k = new AvroKey<PageKey>(key) ;
			AvroValue<PageDetail> v = new AvroValue<PageDetail>(value) ;
			
			collector.collect(k,v) ;
		}

		@Override
		public void close() throws IOException {
			super.close() ;
			mos.close();
		}
	}
	
	
	


	protected static class Reduce extends AvroReducer<PageKey, PageDetail, Pair<PageKey, PageDetail>> {



		@Override
		public void reduce(PageKey key, Iterable<PageDetail> pages,
				AvroCollector<Pair<PageKey, PageDetail>> collector,
				Reporter reporter) throws IOException {

			PageDetail combinedPage = new PageDetail() ;
			combinedPage.setRedirects(new ArrayList<PageSummary>()) ;

			SortedMap<Integer,PageSummary> redirects = new TreeMap<Integer, PageSummary>() ;

			for (PageDetail p: pages) {

				if (p.getId() != null)
					combinedPage.setId(p.getId());
				
				if (p.getTitle() != null)
					combinedPage.setTitle(p.getTitle()) ;

				if (p.getIsRedirect() != null && p.getIsRedirect())
					combinedPage.setIsRedirect(p.getIsRedirect()) ;

				if (p.getRedirectsToTitle() != null)
					combinedPage.setRedirectsToTitle(p.getRedirectsToTitle());
				
				if (p.getRedirectsTo() != null)
					combinedPage.setRedirectsTo(p.getRedirectsTo());

				if (p.getRedirects() != null) {

					for (PageSummary redirect:p.getRedirects()) {

						//only overwrite if previous entry has not been forwarded
						PageSummary existingRedirect = redirects.get(redirect.getId()) ;
						if (existingRedirect == null || !existingRedirect.getForwarded())
							redirects.put(redirect.getId(), redirect) ;
					}
				}
			}

			if (!redirects.isEmpty()) {

				List<PageSummary> redirectList = new ArrayList<PageSummary>() ;
				for (PageSummary redirect:redirects.values()) {

					if (combinedPage.getIsRedirect()) {
						
						//we have received a redirect to a redirect, which needs to be forwarded along (unless already done so)
						if (!redirect.getForwarded())
							reporter.incrCounter(Counter.unforwardedRedirectCount, 1);

					} else {
						//this redirect does not need to be forwarded
						//state that this redirect has been forwarded
						redirect.setForwarded(true) ;
					}

					redirectList.add(redirect) ;

				}

				combinedPage.setRedirects(redirectList);
			}

			//if we don't know the id of the page by this point, then it must be the result of an unresolvable redirect or link (so forget it)
			if (combinedPage.getId() == null)
				return ;
			
			
			//otherwise, collect it
			collector.collect(new Pair<PageKey,PageDetail>(key, combinedPage));
		}
	}
}
