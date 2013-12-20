package org.wikipedia.miner.extract.steps.labelOccurrences;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.mapred.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.wikipedia.miner.extract.model.struct.LabelOccurrences;
import org.wikipedia.miner.extract.steps.Step;
import org.wikipedia.miner.extract.steps.labelOccurrences.CombinerOrReducer.Combiner;
import org.wikipedia.miner.extract.steps.labelOccurrences.CombinerOrReducer.Counts;
import org.wikipedia.miner.extract.steps.labelOccurrences.CombinerOrReducer.Reducer;
import org.wikipedia.miner.extract.steps.labelSenses.LabelSensesStep;
import org.wikipedia.miner.extract.util.UncompletedStepException;
import org.wikipedia.miner.extract.util.XmlInputFormat;

public class LabelOccurrenceStep extends Step{
	
	private static Logger logger = Logger.getLogger(LabelOccurrenceStep.class) ;
	
	public static final String KEY_TOTAL_LABELS = "wm.totalLabels" ;

	private Map<Counts,Long> counts ;
	private LabelSensesStep sensesStep ;
	
	public LabelOccurrenceStep(Path workingDir, LabelSensesStep sensesStep) throws IOException {
		super(workingDir);
		
		this.sensesStep = sensesStep ;
	}

	@Override
	public int run(String[] args) throws Exception {
		
		logger.info("Starting label occurrence step");
		
		if (isFinished()) {
			logger.info(" - already completed");
			loadCounts() ;
			
			return 0 ;
		} else
			reset() ;
		
		
		JobConf conf = new JobConf(LabelOccurrenceStep.class);
		DumpExtractor.configureJob(conf, args) ;
		
		if (sensesStep.getTotalLabels() >= Integer.MAX_VALUE)
			throw new Exception("Waay to many distinct labels (this must be less than " + Integer.MAX_VALUE + ")") ;
		
		conf.setInt(KEY_TOTAL_LABELS, (int)sensesStep.getTotalLabels());

		conf.setJobName("WM: label occurrences");

			
		conf.setMapperClass(Mapper.class);
		conf.setOutputKeyClass(AvroKey.class);
		conf.setOutputValueClass(AvroValue.class);

			
		conf.setInputFormat(XmlInputFormat.class);
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;
			
		FileInputFormat.setInputPaths(conf, conf.get(DumpExtractor.KEY_INPUT_FILE));
		
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_SENTENCE_MODEL)).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);
		
		
		for (FileStatus fs:FileSystem.get(conf).listStatus(sensesStep.getDir())) {

			if (fs.getPath().getName().startsWith("part-")) {
				Logger.getLogger(LabelOccurrenceStep.class).info("Cached labels file " + fs.getPath()) ;
				DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
			}
		}
		
		AvroJob.setCombinerClass(conf, Combiner.class) ;
		AvroJob.setReducerClass(conf, Reducer.class);
		AvroJob.setOutputSchema(conf, Pair.getPairSchema(Schema.create(Type.STRING),LabelOccurrences.getClassSchema()));
		
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
		return "labelOccurrences" ;
	}

	private Path getUnforwardedCountsPath() {
		return new Path(getDir() + Path.SEPARATOR + "unforwarded") ;
	}
	
	private void saveCounts() throws IOException {
		FSDataOutputStream out = getHdfs().create(getUnforwardedCountsPath());
		
		for (Counts c:Counts.values()) {
			
			out.writeUTF(c.name()) ;
			
			Long count = counts.get(c) ;
			if (count != null)
				out.writeLong(count) ;
			else
				out.writeLong(0L) ;
		}
		
		out.close();
	}
	
	private void loadCounts() throws IOException {
		
		counts = new HashMap<Counts,Long>() ;
		
		FSDataInputStream in = getHdfs().open(getUnforwardedCountsPath());
		
		while (in.available() > 0) {
			
			String c = in.readUTF() ;
			
			Long count = in.readLong() ;
			
			counts.put(Counts.valueOf(c), count) ;
		}
	
		in.close() ;
		
	}
	
	
	
	public void finish(RunningJob runningJob) throws IOException {
		
		super.finish(runningJob) ;
	
		counts = new HashMap<Counts,Long>() ;

		for (Counts c:Counts.values()) {
			
			Counters.Counter counter = runningJob.getCounters().findCounter(c) ;
			if (counter != null)
				counts.put(c, counter.getCounter()) ;
			else
				counts.put(c, 0L) ;
		}
		
		saveCounts() ;
		
	}
	
}
