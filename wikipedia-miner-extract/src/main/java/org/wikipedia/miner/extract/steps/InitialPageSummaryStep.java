package org.wikipedia.miner.extract.steps;

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
public class InitialPageSummaryStep extends Configured implements Tool {

	public enum Output {tempPage, tempRedirect, tempRootCategory, tempEditDates} ;
	public enum Counter {articleCount, categoryCount, disambiguationCount, redirectCount, rootCategoryId, rootCategoryCount, unforwardedRedirectCount} ;


	public Counters counters ;

	public int run(String[] args) throws Exception {

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
		FileOutputFormat.setOutputPath(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.page)));

		MultipleOutputs.addNamedOutput(conf, Output.tempRootCategory.name(), TextOutputFormat.class,
				IntWritable.class, Text.class);

		RunningJob runningJob = JobClient.runJob(conf);
		counters = runningJob.getCounters() ;

		return 0;
	}



/*
	public TreeMap<String, Long> updateStats(TreeMap<String, Long> stats) throws Exception {


		if (counters.getCounter(Counter.rootCategoryCount) != 1) {
			throw new Exception ("Could not identify root category") ;
		}

		for (Counter c: Counter.values()) {
			if (c != Counter.rootCategoryCount)
				stats.put(c.name(), counters.getCounter(c)) ;
		}

		return stats ;
	}*/

	private static class Map extends MapReduceBase implements Mapper<LongWritable, Text, PageKey, PageDetail> {

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





		public void map(LongWritable key, Text value, OutputCollector<PageKey, PageDetail> output, Reporter reporter) throws IOException {

			try {
				DumpPage parsedPage = dpp.parsePage(value.toString()) ;

				if (parsedPage != null) {

					String title = Util.normaliseTitle(parsedPage.getTitle()) ;

					PageDetail page = new PageDetail() ;
					page.setNamespace(parsedPage.getNamespace());
					page.setId(parsedPage.getId());
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

						page.setRedirectsTo(targetTitle) ;


						// emit a pair to associate this redirect with target
						PageDetail targetSummary = new PageDetail() ;

						PageSummary redirect = new PageSummary() ;
						redirect.setId(parsedPage.getId());
						redirect.setTitle(targetTitle);

						List<PageSummary> redirects = new ArrayList<PageSummary>() ;
						redirects.add(redirect) ;
						targetSummary.setRedirects(redirects);

						output.collect(new PageKey(parsedPage.getNamespace(), targetTitle), targetSummary);

						break ;
					default:
						//for all other page types, do nothing
						return ;
					}


					output.collect(new PageKey(parsedPage.getNamespace(), title), page);
				}

			} catch (Exception e) {
				Logger.getLogger(Map.class).error("Caught exception", e) ;
			}
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
			
			SortedMap<Integer,PageSummary> redirects = new TreeMap<Integer, PageSummary>() ;
			
			for (PageDetail p: pages) {
								
				if (p.getId() != null)
					combinedPage.setId(p.getId());
				
				if (p.getIsRedirect())
					combinedPage.setIsRedirect(p.getIsRedirect()) ;
				
				if (p.getRedirectsTo() != null)
					combinedPage.setRedirectsTo(p.getRedirectsTo());
				
				if (p.getRedirects() != null) {
					
					for (PageSummary redirect:p.getRedirects()) {
						
						//assume this has not been forwarded, unless explicitly stated
						if (redirect.getIsForwarded() == null)
							redirect.setIsForwarded(false);
						
						//only overwrite if previous entry has not been forwarded
						PageSummary existingRedirect = redirects.get(redirect.getId()) ;
						if (existingRedirect == null || !existingRedirect.getIsForwarded())
							redirects.put(redirect.getId(), redirect) ;
					}
				}
			}
			
			if (!redirects.isEmpty()) {
		
				List<PageSummary> redirectList = new ArrayList<PageSummary>() ;
				for (PageSummary redirect:redirects.values()) {
	
					if (combinedPage.getIsRedirect()) {
						if (redirect.getIsForwarded() == false)
							reporter.incrCounter(Counter.unforwardedRedirectCount, 1);
						
					} else {
						//remove any mention of forwarding (because this is not a redirect)
						combinedPage.setIsRedirect(null);
					}
					
					redirectList.add(redirect) ;
					
				}
								
				combinedPage.setRedirects(redirectList);
			}
				
			collector.collect(new Pair<PageKey,PageDetail>(key, combinedPage));
			
			
		}
	}
}
