package org.wikipedia.miner.extract.steps.pageSummary;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.LabelSummary;
import org.wikipedia.miner.extract.model.struct.LinkSummary;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.model.struct.PageSummary;
import org.wikipedia.miner.extract.steps.IterativeStep;
import org.wikipedia.miner.extract.steps.pageSummary.CombinerOrReducer.Combiner;
import org.wikipedia.miner.extract.steps.pageSummary.CombinerOrReducer.Reducer;
import org.wikipedia.miner.extract.util.UncompletedStepException;
import org.wikipedia.miner.extract.util.XmlInputFormat;


/**
 * @author dmilne
 *
 * This step produces PageDetail structures. 
 * It needs to be run multiple times for the PageDetail structures to be completed (they get built gradually). 
 * 
 * Completion is indicated when all Unforwarded counters reach 0. 
 * 
 * The number of iterations needed is bounded by the longest chain of redirects
 *  (i.e. a redirect pointing to a redirect pointing to a redirect pointing to...)
 * 
 * The first iteration reads directly from the xml dump. 
 * Subsequent iterations read from the results of the previous iteration.
 * 
 * The page summaries will be missing namespace and title fields, because they are found in the page keys (so repeating them would be wasteful)
 *
 */
public class PageSummaryStep extends IterativeStep {

	private static Logger logger = Logger.getLogger(PageSummaryStep.class) ;
	
	public enum SummaryPageType {article, category, disambiguation, articleRedirect, categoryRedirect, unparseable} ; 
	public enum Unforwarded {redirect,linkIn,linkOut,parentCategory,childCategory,childArticle} ; 


	private Map<Unforwarded,Long> unforwardedCounts ;


	
	public PageSummaryStep(Path workingDir, int iteration) throws IOException {
		super(workingDir, iteration);
		

	}
	

	public boolean furtherIterationsRequired() {

		for (Long count:unforwardedCounts.values()) {
			if (count > 0)
				return true ;
		}

		return false ;

	}

	public static PageSummary clone(PageSummary summary) {

		return PageSummary.newBuilder(summary).build() ;
	}
	
	public static LinkSummary clone(LinkSummary summary) {

		return LinkSummary.newBuilder(summary).build() ;
	}
	
	public static PageDetail clone(PageDetail pageDetail) {

		return PageDetail.newBuilder(pageDetail).build() ;
	}

	public static PageDetail buildEmptyPageDetail() {

		PageDetail p = new PageDetail() ;
		p.setIsDisambiguation(false);
		p.setSentenceSplits(new ArrayList<Integer>());
		p.setRedirects(new ArrayList<PageSummary>()) ;
		p.setLinksIn(new ArrayList<LinkSummary>());
		p.setLinksOut(new ArrayList<LinkSummary>());
		p.setParentCategories(new ArrayList<PageSummary>());
		p.setChildCategories(new ArrayList<PageSummary>());
		p.setChildArticles(new ArrayList<PageSummary>());
		p.setLabels(new HashMap<CharSequence,LabelSummary>()) ;

		return p ;
	}


	@Override
	public int run(String[] args) throws UncompletedStepException, IOException {
		
		
		logger.info("Starting page summary step (iteration " + getIteration() + ")");
		
		if (isFinished()) {
			logger.info(" - already completed");
			loadUnforwardedCounts() ;
			
			return 0 ;
		} else
			reset() ;
		
		JobConf conf = new JobConf(PageSummaryStep.class);
		DumpExtractor.configureJob(conf, args) ;

		conf.setJobName("WM: page summary (" + getIteration() + ")");
		
		if (getIteration() == 0) {
			
			conf.setMapperClass(InitialMapper.class);

			conf.setOutputKeyClass(AvroKey.class);
			conf.setOutputValueClass(AvroValue.class);

			
			conf.setInputFormat(XmlInputFormat.class);
			conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
			conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;
			
			FileInputFormat.setInputPaths(conf, conf.get(DumpExtractor.KEY_INPUT_FILE));
			DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_SENTENCE_MODEL)).toUri(), conf);
			
			
		} else {
			
			AvroJob.setMapperClass(conf, SubsequentMapper.class);
			AvroJob.setInputSchema(conf, Pair.getPairSchema(PageKey.getClassSchema(),PageDetail.getClassSchema()));
		
			FileInputFormat.setInputPaths(conf, getWorkingDir() + Path.SEPARATOR + "pageSummary_" + (getIteration()-1));
			
		}
		
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);
		
		AvroJob.setCombinerClass(conf, Combiner.class) ;
		AvroJob.setReducerClass(conf, Reducer.class);
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(PageKey.getClassSchema(),PageDetail.getClassSchema()));
		
		FileOutputFormat.setOutputPath(conf, getDir());

		
		RunningJob runningJob = JobClient.runJob(conf);
	
		if (runningJob.getJobState() == JobStatus.SUCCEEDED) {	
			finish(runningJob) ;
			return 0 ;
		}
		
		throw new UncompletedStepException() ;
	}


	@Override
	public String getDirName(int iteration) {
		
		return "pageSummary_" + iteration ;
	}
	
	private Path getUnforwardedCountsPath() {
		return new Path(getDir() + Path.SEPARATOR + "unforwarded") ;
	}
	
	private void saveUnforwardedCounts() throws IOException {
		FSDataOutputStream out = getHdfs().create(getUnforwardedCountsPath());
		
		for (Unforwarded u:Unforwarded.values()) {
			
			out.writeUTF(u.name()) ;
			
			Long count = unforwardedCounts.get(u) ;
			if (count != null)
				out.writeLong(count) ;
			else
				out.writeLong(0L) ;
		}
		
		out.close();
	}
	
	private void loadUnforwardedCounts() throws IOException {
		
		unforwardedCounts = new HashMap<Unforwarded,Long>() ;
		
		
		FSDataInputStream in = getHdfs().open(getUnforwardedCountsPath());
		
		while (in.available() > 0) {
			
			String u = in.readUTF() ;
			
			Long count = in.readLong() ;
			
			unforwardedCounts.put(Unforwarded.valueOf(u), count) ;
		}
	
		in.close() ;
		
	}
	
	
	
	public void finish(RunningJob runningJob) throws IOException {
		
		super.finish(runningJob) ;
	
		unforwardedCounts = new HashMap<Unforwarded,Long>() ;

		for (Unforwarded u:Unforwarded.values()) {
			
			Counters.Counter counter = runningJob.getCounters().findCounter(u) ;
			if (counter != null)
				unforwardedCounts.put(u, counter.getCounter()) ;
			else
				unforwardedCounts.put(u, 0L) ;
		}
		
		saveUnforwardedCounts() ;
		
	}

}
