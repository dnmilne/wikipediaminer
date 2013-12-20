package org.wikipedia.miner.extract.steps.sortedPages;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.AvroReducer;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.model.struct.PageDetail;
import org.wikipedia.miner.extract.model.struct.PageKey;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.pageDepth.PageDepthStep;
import org.wikipedia.miner.extract.steps.pageSummary.PageSummaryStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;

/**
 * In this step we sort page summaries produced by PageSummaryStep by id (they were previously sorted by namespace:title)
 * We also inject titles and namespaces into each page summary (they were previously omitted because they are found in keys, and repeating would be wasteful)
 */
public class PageSortingStep extends Step {
	
	private static Logger logger = Logger.getLogger(PageSortingStep.class) ;
	
	PageSummaryStep finalPageSummaryStep ;

	public PageSortingStep(Path workingDir, PageSummaryStep finalPageSummaryStep) throws IOException {
		super(workingDir);
		this.finalPageSummaryStep = finalPageSummaryStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting page sorting step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			return 0 ;
		} else {
			reset() ;
		}
		
		JobConf conf = new JobConf(PageDepthStep.class);
		DumpExtractor.configureJob(conf, args) ;

		conf.setJobName("WM: sorted pages");
		
		
		FileInputFormat.setInputPaths(conf, getWorkingDir() + Path.SEPARATOR + finalPageSummaryStep.getDirName());
		AvroJob.setInputSchema(conf, Pair.getPairSchema(PageKey.getClassSchema(),PageDetail.getClassSchema()));
			
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT),PageDetail.getClassSchema()));
		
		AvroJob.setMapperClass(conf, Mapper.class);
		AvroJob.setReducerClass(conf, Reducer.class);
		
		FileOutputFormat.setOutputPath(conf, getDir());
		
		RunningJob runningJob = JobClient.runJob(conf);
	
		if (runningJob.getJobState() == JobStatus.SUCCEEDED) {	
			finish(runningJob) ;
			return 0 ;
		}
		
		throw new UncompletedStepException() ;
	}

	@Override
	public String getDirName() {
		return "sortedPages" ;
	}
	
	
	public static class Mapper extends AvroMapper<Pair<PageKey, PageDetail>, Pair<Integer, PageDetail>>{
		
		@Override
		public void map(Pair<PageKey, PageDetail> pair,
				AvroCollector<Pair<Integer, PageDetail>> collector,
				Reporter reporter) throws IOException {
			
			PageKey key = pair.key() ;
			PageDetail page = pair.value() ;
			
			
			page.setNamespace(key.getNamespace());
			page.setTitle(key.getTitle());
			
			collector.collect(new Pair<Integer, PageDetail>(page.getId(), page));
		}
	}
	
	
	public static class Reducer extends AvroReducer<Integer, PageDetail, Pair<Integer,PageDetail>>{
		
		@Override
		public void reduce(Integer pageId, Iterable<PageDetail> pages,
				AvroCollector<Pair<Integer,PageDetail>> collector,
				Reporter reporter) throws IOException {
			
			for (PageDetail page:pages)
				collector.collect(new Pair<Integer, PageDetail>(page.getId(), page));
		}
	}
	
}
