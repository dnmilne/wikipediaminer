package org.wikipedia.miner.extract.steps;

import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.record.*;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;

import org.wikipedia.miner.db.struct.*;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.DumpExtractor.ExtractionStep;
import org.wikipedia.miner.extract.model.struct.*;




public class CategoryLinkSummaryStep extends Configured implements Tool {
	
	//public enum LinksToSummarize {pageLinks, categoryParents, articleParents} ;
	public enum Output {categoryParents, articleParents, childCategories, childArticles } ;
	public static final String KEY_LINKS_TO_SUMMARIZE = "wm.linksToSummarize" ;
	
	private ExtractionStep linksToSummarize ;
		
	public CategoryLinkSummaryStep(ExtractionStep linksToSummarize) {
		
		this.linksToSummarize = linksToSummarize ;
		
	}

	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(CategoryLinkSummaryStep.class);
		DumpExtractor.configureJob(conf, args) ;
		conf.set(KEY_LINKS_TO_SUMMARIZE, linksToSummarize.name()) ;
		
		conf.setJobName("WM: summarize " + linksToSummarize.name()) ;
		
		conf.setOutputKeyClass(ExLinkKey.class);
		conf.setOutputValueClass(DbIntList.class);

		conf.setMapperClass(CategoryLinkSummaryMapper.class);
		conf.setCombinerClass(CategoryLinkSummaryReducer.class) ;
		conf.setReducerClass(CategoryLinkSummaryReducer.class) ;

		// set up input

		conf.setInputFormat(TextInputFormat.class);
		switch (linksToSummarize) {
		
		case categoryParent: 
			FileInputFormat.setInputPaths(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.labelSense) + "/" + LabelSensesStep.Output.tempCategoryParent.name() + "*"));
			break ;
		case articleParent: 
			FileInputFormat.setInputPaths(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.labelSense) + "/" + LabelSensesStep.Output.tempArticleParent.name() + "*"));
			break ;	
		}
		
		//set up output

		conf.setOutputFormat(CategoryLinkSummaryOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(linksToSummarize)));

		JobClient.runJob(conf);
		return 0;
	}
	
	private static class CategoryLinkSummaryMapper extends MapReduceBase implements Mapper<LongWritable, Text, ExLinkKey, DbIntList> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<ExLinkKey, DbIntList> output, Reporter reporter) throws IOException {
			
			String values[] = value.toString().split(",") ;
			
			int fromId = Integer.parseInt(values[0]) ;
			int toId = Integer.parseInt(values[1]) ;
			
			ArrayList<Integer> out = new ArrayList<Integer>() ;
			out.add(toId) ;
			output.collect(new ExLinkKey(fromId, true), new DbIntList(out)) ;
			
			ArrayList<Integer> in = new ArrayList<Integer>() ;
			in.add(fromId) ;
			output.collect(new ExLinkKey(toId, false), new DbIntList(in)) ;
		}
	}
	
	
	public static class CategoryLinkSummaryReducer extends MapReduceBase implements Reducer<ExLinkKey, DbIntList, ExLinkKey, DbIntList> {

		public void reduce(ExLinkKey key, Iterator<DbIntList> values, OutputCollector<ExLinkKey, DbIntList> output, Reporter reporter) throws IOException {

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
	
	
	public static class CategoryLinkSummaryOutputFormat extends TextOutputFormat<ExLinkKey, DbIntList> {

		public RecordWriter<ExLinkKey, DbIntList> getRecordWriter(FileSystem ignored,
				JobConf job,
				String name,
				Progressable progress)
				throws IOException {
			
			String nameOut = null ;
			String nameIn = null ;
			
			ExtractionStep lts = ExtractionStep.valueOf(job.get(KEY_LINKS_TO_SUMMARIZE)) ;
			
			switch (lts) {
			
			case categoryParent:
				nameOut = name.replace("part", Output.categoryParents.name()) ;
				nameIn = name.replace("part", Output.childCategories.name()) ;
				break ;
			case articleParent:
				nameOut = name.replace("part", Output.articleParents.name()) ;
				nameIn = name.replace("part", Output.childArticles.name()) ;
				break ;	
			}
			 
			Path fileOut = FileOutputFormat.getTaskOutputPath(job, nameOut);
			FileSystem fsOut = fileOut.getFileSystem(job);
			FSDataOutputStream streamOut = fsOut.create(fileOut, progress);
			
			Path fileIn = FileOutputFormat.getTaskOutputPath(job, nameIn);
			FileSystem fsIn = fileIn.getFileSystem(job);
			FSDataOutputStream streamIn = fsIn.create(fileIn, progress);

			return new CategoryLinkSummaryRecordWriter(streamOut, streamIn);
		}	
		
		public static class CategoryLinkSummaryRecordWriter implements RecordWriter<ExLinkKey, DbIntList> {

			public OutputStream linksOut_outStream ;
			public OutputStream linksIn_outStream ;
			
			public CategoryLinkSummaryRecordWriter(OutputStream linksOut_outStream, OutputStream linksIn_outStream) {
				this.linksOut_outStream = linksOut_outStream ; 
				this.linksIn_outStream = linksIn_outStream ;
			}
			
			public synchronized void write(ExLinkKey key, DbIntList value) throws IOException {
				
				ArrayList<Integer> links = value.getValues() ;
				Collections.sort(links) ;
				
				OutputStream stream ;
				if (key.getIsOut())
					stream = linksOut_outStream ;
				else
					stream = linksIn_outStream ;
				
				CsvRecordOutput csvOutput = new CsvRecordOutput(stream);			
				csvOutput.writeInt(key.getId(), "id") ;
				csvOutput.startVector(links, "links") ;
				for (int link:links) 
					csvOutput.writeInt(link, "link") ;
				
				csvOutput.endVector(links, "links") ;
				
				stream.write('\n') ;
			}
		
			public synchronized void close(Reporter reporter) throws IOException {
				linksOut_outStream.close();
				linksIn_outStream.close() ;
			}
		}
	}
	
}
