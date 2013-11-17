package org.wikipedia.miner.extract.steps;


import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Vector;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import org.wikipedia.miner.db.struct.DbIntList;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.DumpExtractor.ExtractionStep;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.model.Page.PageType;


/**
 * The second step in the extraction process.
 * 
 * This produces the following sequence files (in <i>&lt;ouput_dir&gt;/step2/</i>)
 * <ul>
 * <li><b>redirectTargetsBySource-xxxxx</b> - csv files associating Integer id for redirect with Integer id of its target.</li>
 * <li><b>redirectSourcesByTarget-xxxxx</b> - csv files associating Integer id for redirect target with a list of Integer ids of sources.</li>
 * </ul>
 */
@SuppressWarnings("deprecation")
public class RedirectStep extends Configured implements Tool {

	public enum Output {redirectTargetsBySource, redirectSourcesByTarget} ;
	

	@Override
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(RedirectStep.class);
		DumpExtractor.configureJob(conf, args) ;
		
		conf.setJobName("WM: resolve redirects");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DbIntList.class);		

		conf.setMapperClass(Step2Mapper.class);
		conf.setCombinerClass(Step2Reducer.class) ;
		conf.setReducerClass(Step2Reducer.class) ;

		// set up input

		conf.setInputFormat(TextInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.page) + "/" + PageStep.Output.tempRedirect + "*"));

		//set up output

		conf.setOutputFormat(RedirectOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.redirect)));

		//set up distributed cache

		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);

		//cache page files created in previous step, so we can look up pages by title
		Path pageStepPath = new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.page)) ;
		for (FileStatus fs:FileSystem.get(conf).listStatus(pageStepPath)) {

			if (fs.getPath().getName().startsWith(PageStep.Output.tempPage.name())) {
				Logger.getLogger(RedirectStep.class).info("Cached page file " + fs.getPath()) ;
				DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
			}
		}
		
		MultipleOutputs.addNamedOutput(conf, Output.redirectTargetsBySource.name(), TextOutputFormat.class,
				IntWritable.class, IntWritable.class);
		
		conf.set("mapred.textoutputformat.separator", ",");
		
		//run job
		JobClient.runJob(conf);
		return 0;
	}

	/**
	 *	Takes xml markup of pages (one page element per record) and emits 
	 *		-key: redirect id
	 *		-value: redirect target id
	 */
	private static class Step2Mapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DbIntList> {

		private LanguageConfiguration lc ;
		private SiteInfo si ;
		
		private MultipleOutputs mos ;
		
		Vector<Path> pageFiles = new Vector<Path>() ;
		private TObjectIntHashMap<String> articlesByTitle = null ;

		@Override
		public void configure(JobConf job) {

			HashSet<PageType> pageTypesToCache = new HashSet<PageType>() ;
			pageTypesToCache.add(PageType.article) ;
			pageTypesToCache.add(PageType.redirect) ;
			pageTypesToCache.add(PageType.disambiguation) ;

			try {

				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(job);

				for (Path cf:cacheFiles) {

					if (cf.getName().equals(new Path(DumpExtractor.OUTPUT_SITEINFO).getName())) {
						si = new SiteInfo(cf) ;
					}

					if (cf.getName().equals(new Path(job.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
						lc = new LanguageConfiguration(job.get(DumpExtractor.KEY_LANG_CODE), cf) ;
					}

					if (cf.getName().startsWith(PageStep.Output.tempPage.name())) {
						Logger.getLogger(Step2Mapper.class).info("Located cached page file " + cf.toString()) ;
						pageFiles.add(cf) ;
					}
				}

				if (si == null) 
					throw new Exception("Could not locate '" + DumpExtractor.OUTPUT_SITEINFO + "' in DistributedCache") ;

				if (lc == null) 
					throw new Exception("Could not locate '" + job.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

				if (pageFiles.isEmpty())
					throw new Exception("Could not gather page summary files produced in step 1") ;
				
				mos = new MultipleOutputs(job);

			} catch (Exception e) {
				Logger.getLogger(Step2Mapper.class).error("Could not configure mapper", e);
				System.exit(1) ;
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DbIntList> output, Reporter reporter) throws IOException {

			try {
				
				//set up articlesByTitle if this hasn't been done already
				//this is done during map rather than configure, so that we can report progress
				//and stop hadoop from declaring a timeout.
				if (articlesByTitle == null) {
					
					HashSet<PageType> articleTypesToCache = new HashSet<PageType>() ;
					articleTypesToCache.add(PageType.article) ;
					articleTypesToCache.add(PageType.redirect) ;
					articleTypesToCache.add(PageType.disambiguation) ;
					
					articlesByTitle = new TObjectIntHashMap<String>() ;
					
					for (Path p:pageFiles) {
						articlesByTitle = Util.gatherPageIdsByTitle(p, articleTypesToCache, articlesByTitle, reporter) ;
					}
				}
				
				String line = value.toString() ;

				int pos = line.indexOf(',') ;

				int sourceId = Integer.parseInt(line.substring(0,pos)) ;
				String targetTitle = line.substring(pos+1) ;
				Integer targetId = Util.getTargetId(targetTitle, articlesByTitle, null) ;

				if (targetId == null)
					Logger.getLogger(Step2Mapper.class).warn("Could not identify id for redirect target '" + targetTitle + "'") ;
				else {
					mos.getCollector(Output.redirectTargetsBySource.name(), reporter).collect(new IntWritable(sourceId), new IntWritable(targetId));
					
					ArrayList<Integer> sources = new ArrayList<Integer>() ;
					sources.add(sourceId) ;
					output.collect(new IntWritable(targetId), new DbIntList(sources)) ;
				}

			} catch (Exception e) {
				Logger.getLogger(Step2Mapper.class).error("Caught exception while processing redirect '" + value + "'", e) ;
			}
		}
		
		@Override
		public void close() throws IOException {
			super.close() ;
			mos.close();
		}
	}
	
	public static class Step2Reducer extends MapReduceBase implements Reducer<IntWritable, DbIntList, IntWritable, DbIntList> {

		public void reduce(IntWritable key, Iterator<DbIntList> values, OutputCollector<IntWritable, DbIntList> output, Reporter reporter) throws IOException {

			ArrayList<Integer> valueIds = new ArrayList<Integer>() ;
	
			while (values.hasNext()) {
				ArrayList<Integer> ls = values.next().getValues() ;
				for (Integer i:ls) {
					valueIds.add(i) ;
				}
			}

			output.collect(key, new DbIntList(valueIds));
		}
	}
	
	
	


	private static class RedirectOutputFormat extends FileOutputFormat<IntWritable, DbIntList> {

		public RecordWriter<IntWritable, DbIntList> getRecordWriter(FileSystem ignored,
				JobConf job,
				String name,
				Progressable progress)
				throws IOException {

			String newName = name.replace("part", Output.redirectSourcesByTarget.name()) ;

			Path file = FileOutputFormat.getTaskOutputPath(job, newName);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new RedirectRecordWriter(fileOut);
		}

		public static class RedirectRecordWriter implements RecordWriter<IntWritable, DbIntList> {

			public DataOutputStream outStream ;

			public RedirectRecordWriter(DataOutputStream out) {
				this.outStream = out ; 
			}

			public synchronized void write(IntWritable key, DbIntList value) throws IOException {
				
				ArrayList<Integer> sources = value.getValues() ;
				Collections.sort(sources) ;

				CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);
	
				csvOutput.writeInt(key.get(), "target") ;
				csvOutput.startVector(sources, "sources") ;
				for (int source:sources) 
					csvOutput.writeInt(source, "link") ;
				
				csvOutput.endVector(sources, "sources") ;
				
				outStream.write('\n') ;
			}

			public synchronized void close(Reporter reporter) throws IOException {
				outStream.close();
			}
		}
	}

}
