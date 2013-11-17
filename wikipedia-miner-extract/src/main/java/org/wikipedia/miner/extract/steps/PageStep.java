package org.wikipedia.miner.extract.steps;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.record.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import org.wikipedia.miner.db.struct.*;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.DumpExtractor.ExtractionStep;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.extract.util.XmlInputFormat;

/**
 * The first step in the extraction process.
 * 
 * This produces the following sequence files (in <i>&lt;ouput_dir&gt;/step1/</i>)
 * <ul>
 * <li><b>tempPage-xxxxx</b> - a csv file associating Integer id with DbPage.</li>
 * <li><b>tempRedirect-xxxxx</b> - a csv file associating Integer id with the title of a redirect target.</li>
 * </ul>
 */
@SuppressWarnings("deprecation")
public class PageStep extends Configured implements Tool {
	
	public enum Output {tempPage, tempRedirect, tempRootCategory, tempEditDates} ;
	public enum Counter {articleCount, categoryCount, disambiguationCount, redirectCount, rootCategoryId, rootCategoryCount} ;
	
	
	public Counters counters ;
	
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(PageStep.class);
		DumpExtractor.configureJob(conf, args) ;
		
		conf.setJobName("WM: gather pages");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DbPage.class);

		conf.setMapperClass(Step1Mapper.class);

		conf.setInputFormat(XmlInputFormat.class);
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;

		conf.setOutputFormat(PageOutputFormat.class);

		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);

		FileInputFormat.setInputPaths(conf, conf.get(DumpExtractor.KEY_INPUT_FILE));
		FileOutputFormat.setOutputPath(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.page)));

		MultipleOutputs.addNamedOutput(conf, Output.tempRedirect.name(), TextOutputFormat.class,
				IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(conf, Output.tempRootCategory.name(), TextOutputFormat.class,
				IntWritable.class, Text.class);
		MultipleOutputs.addNamedOutput(conf, Output.tempEditDates.name(), TextOutputFormat.class,
				IntWritable.class, LongWritable.class);
		

		conf.set("mapred.textoutputformat.separator", ",");

		RunningJob runningJob = JobClient.runJob(conf);
		counters = runningJob.getCounters() ;
						
		return 0;
	}
	
	public TreeMap<String, Long> updateStats(TreeMap<String, Long> stats) throws Exception {
		
		
		if (counters.getCounter(Counter.rootCategoryCount) != 1) {
			throw new Exception ("Could not identify root category") ;
		}
		
		for (Counter c: Counter.values()) {
			if (c != Counter.rootCategoryCount)
				stats.put(c.name(), counters.getCounter(c)) ;
		}
		
		return stats ;
	}
	
	private static class Step1Mapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DbPage> {

		private LanguageConfiguration lc ;
		//private SiteInfo si ;
		private DumpPageParser dpp ;

		private MultipleOutputs mos ;

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
				
				
				
				
				

			} catch (Exception e) {
				Logger.getLogger(Step1Mapper.class).error("Could not configure mapper", e);
			}
		}

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DbPage> output, Reporter reporter) throws IOException {

			try {
				DumpPage dp = dpp.parsePage(value.toString()) ;

				if (dp != null) {
					output.collect(new IntWritable(dp.getId()), new DbPage(dp.getTitle(),dp.getType().ordinal(), -1));
					
					switch (dp.getType()) {
					
					case article :
						reporter.incrCounter(Counter.articleCount, 1);
						break ;
					case category :
						reporter.incrCounter(Counter.categoryCount, 1);
						
						
						if (Util.normaliseTitle(dp.getTitle()).equals(Util.normaliseTitle(lc.getRootCategoryName()))) {
							
							reporter.incrCounter(Counter.rootCategoryCount, 1);
							reporter.incrCounter(Counter.rootCategoryId, dp.getId()) ;
						}
							
						break ;
					case disambiguation :
						reporter.incrCounter(Counter.disambiguationCount, 1);
						break ;
					case redirect :
						reporter.incrCounter(Counter.redirectCount, 1);
						mos.getCollector(Output.tempRedirect.name(), reporter).collect(new IntWritable(dp.getId()), new Text(dp.getTarget()));
						break ;
					}
					
					if (dp.getLastEdited() != null) {	
						mos.getCollector(Output.tempEditDates.name(), reporter).collect(new IntWritable(dp.getId()), new LongWritable(dp.getLastEdited().getTime()));
					}
				}

			} catch (Exception e) {
				Logger.getLogger(Step1Mapper.class).error("Caught exception", e) ;
			}
		}
		
		@Override
		public void close() throws IOException {
			super.close() ;
			mos.close();
		}
	}


	private static class PageOutputFormat extends FileOutputFormat<IntWritable, DbPage> {

		public RecordWriter<IntWritable, DbPage> getRecordWriter(FileSystem ignored,
				JobConf job,
				String name,
				Progressable progress)
				throws IOException {

			String newName = name.replace("part", Output.tempPage.name()) ;

			Path file = FileOutputFormat.getTaskOutputPath(job, newName);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new PageRecordWriter(fileOut);
		}

		public static class PageRecordWriter implements RecordWriter<IntWritable, DbPage> {

			public DataOutputStream outStream ;

			public PageRecordWriter(DataOutputStream out) {
				this.outStream = out ; 
			}

			public synchronized void write(IntWritable key, DbPage value) throws IOException {

				CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);

				csvOutput.writeInt(key.get(), "id") ;
				value.serialize(csvOutput) ;
			}

			public synchronized void close(Reporter reporter) throws IOException {
				outStream.close();
			}
		}
	}

}
