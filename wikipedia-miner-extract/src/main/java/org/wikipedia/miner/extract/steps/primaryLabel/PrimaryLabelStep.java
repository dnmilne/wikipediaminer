package org.wikipedia.miner.extract.steps.primaryLabel;

import java.io.IOException;
import java.util.ArrayList;

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
import org.wikipedia.miner.extract.model.struct.LabelSense;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;
import org.wikipedia.miner.extract.model.struct.PrimaryLabels;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.labelSenses.LabelSensesStep;
import org.wikipedia.miner.extract.steps.pageDepth.PageDepthStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;


public class PrimaryLabelStep extends Step {

	private static Logger logger = Logger.getLogger(PrimaryLabelStep.class) ;
	
	private LabelSensesStep labelSensesStep ;
	
	public PrimaryLabelStep(Path workingDir, LabelSensesStep labelSensesStep) throws IOException {
		super(workingDir);
		
		this.labelSensesStep = labelSensesStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting primary label step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			
			return 0 ;
		} else {
			reset() ;
		}
		
		JobConf conf = new JobConf(PageDepthStep.class);
		DumpExtractor.configureJob(conf, args) ;

		conf.setJobName("WM: primary labels");
		
		
		
		
		FileInputFormat.setInputPaths(conf, getWorkingDir() + Path.SEPARATOR + labelSensesStep.getDirName());
		AvroJob.setInputSchema(conf, Pair.getPairSchema(Schema.create(Type.STRING),LabelSenseList.getClassSchema()));
			
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.INT), PrimaryLabels.getClassSchema()));
		
		AvroJob.setMapperClass(conf, Mapper.class);
		AvroJob.setCombinerClass(conf, Reducer.class);
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
		return "primaryLabels" ;
	}
	
	
	
	
	public static class Mapper extends AvroMapper<Pair<CharSequence, LabelSenseList>, Pair<Integer, PrimaryLabels>>{
		
		@Override
		public void map(Pair<CharSequence, LabelSenseList> pair,
				AvroCollector<Pair<Integer, PrimaryLabels>> collector,
				Reporter reporter) throws IOException {
			
			CharSequence label = pair.key() ;
			LabelSenseList senses = pair.value() ;
			
			if (senses.getSenses().isEmpty())
				return ;
			
			LabelSense firstSense = senses.getSenses().get(0) ;
			
			ArrayList<CharSequence> primaryLabels = new ArrayList<CharSequence>() ;
			primaryLabels.add(label) ;
			
			collector.collect(new Pair<Integer, PrimaryLabels>(firstSense.getId(), new PrimaryLabels(primaryLabels)));
		}
	}
	
	public static class Reducer extends AvroReducer<Integer, PrimaryLabels, Pair<Integer,PrimaryLabels>>{
		
		@Override
		public void reduce(Integer pageId, Iterable<PrimaryLabels> partials,
				AvroCollector<Pair<Integer, PrimaryLabels>> collector,
				Reporter reporter) throws IOException {
			
			ArrayList<CharSequence> primaryLabels = new ArrayList<CharSequence>() ;
			
			for (PrimaryLabels partial:partials) {
				
				PrimaryLabels clone = PrimaryLabels.newBuilder(partial).build() ;
				primaryLabels.addAll(clone.getLabels()) ;
			}
			
			collector.collect(new Pair<Integer, PrimaryLabels>(pageId, new PrimaryLabels(primaryLabels)));
		}
	}
	
}
