package org.wikipedia.miner.extract.steps;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.record.CsvRecordInput;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.wikipedia.miner.db.struct.DbLinkLocation;
import org.wikipedia.miner.db.struct.DbLinkLocationList;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.DumpExtractor.ExtractionStep;
import org.wikipedia.miner.extract.model.struct.ExLinkKey;

public class PageLinkSummaryStep extends Configured implements Tool {
	
	public enum Output {pageLinkOut, pageLinkIn} ;
		
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(CategoryLinkSummaryStep.class);
		DumpExtractor.configureJob(conf, args) ;
		
		conf.setJobName("WM: summarize pagelinks") ;
		
		conf.setOutputKeyClass(ExLinkKey.class);
		conf.setOutputValueClass(DbLinkLocationList.class);

		conf.setMapperClass(PageLinkSummaryMapper.class);
		conf.setCombinerClass(PageLinkSummaryReducer.class) ;
		conf.setReducerClass(PageLinkSummaryReducer.class) ;

		// set up input

		conf.setInputFormat(TextInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.labelSense) + "/" + LabelSensesStep.Output.tempPageLink.name() + "*"));
				
		//set up output

		conf.setOutputFormat(PageLinkSummaryOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.pageLink)));

		JobClient.runJob(conf);
		return 0;
	}
	
	private static class PageLinkSummaryMapper extends MapReduceBase implements Mapper<LongWritable, Text, ExLinkKey, DbLinkLocationList> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<ExLinkKey, DbLinkLocationList> output, Reporter reporter) throws IOException {
			
			CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((value.toString() + "\n").getBytes("UTF-8"))) ;

			int fromId = cri.readInt(null) ;
			DbLinkLocation toIdAndLocation = new DbLinkLocation() ;
			toIdAndLocation.deserialize(cri) ;
			
			int toId = toIdAndLocation.getLinkId() ;
			DbLinkLocation fromIdAndLocation = new DbLinkLocation(fromId, toIdAndLocation.getSentenceIndexes()) ;
					
			ArrayList<DbLinkLocation> out = new ArrayList<DbLinkLocation>() ;
			out.add(toIdAndLocation) ;
			output.collect(new ExLinkKey(fromId, true), new DbLinkLocationList(out)) ;
			
			ArrayList<DbLinkLocation> in = new ArrayList<DbLinkLocation>() ;
			in.add(fromIdAndLocation) ;
			output.collect(new ExLinkKey(toId, false), new DbLinkLocationList(in)) ;
		}
	}
	
	
	public static class PageLinkSummaryReducer extends MapReduceBase implements Reducer<ExLinkKey, DbLinkLocationList, ExLinkKey, DbLinkLocationList> {

		public void reduce(ExLinkKey key, Iterator<DbLinkLocationList> values, OutputCollector<ExLinkKey, DbLinkLocationList> output, Reporter reporter) throws IOException {

			ArrayList<DbLinkLocation> collectedValues = new ArrayList<DbLinkLocation>() ;
	
			while (values.hasNext()) {
				DbLinkLocationList v = values.next() ;
				collectedValues.addAll(v.getLinkLocations()) ;
			}

			output.collect(key, new DbLinkLocationList(collectedValues));
		}
	}
	
	
	protected static class PageLinkSummaryOutputFormat extends TextOutputFormat<ExLinkKey, DbLinkLocationList> {

		public RecordWriter<ExLinkKey, DbLinkLocationList> getRecordWriter(FileSystem ignored,
				JobConf job,
				String name,
				Progressable progress)
				throws IOException {
			
			String nameOut = name.replace("part", Output.pageLinkOut.name()) ;
			String nameIn = name.replace("part", Output.pageLinkIn.name()) ;
					 
			Path fileOut = FileOutputFormat.getTaskOutputPath(job, nameOut);
			FileSystem fsOut = fileOut.getFileSystem(job);
			FSDataOutputStream streamOut = fsOut.create(fileOut, progress);
			
			Path fileIn = FileOutputFormat.getTaskOutputPath(job, nameIn);
			FileSystem fsIn = fileIn.getFileSystem(job);
			FSDataOutputStream streamIn = fsIn.create(fileIn, progress);

			return new LinkSummaryRecordWriter(streamOut, streamIn);
		}	
		
		protected static class LinkSummaryRecordWriter implements RecordWriter<ExLinkKey, DbLinkLocationList> {

			protected OutputStream linksOut_outStream ;
			protected OutputStream linksIn_outStream ;
			
			public LinkSummaryRecordWriter(OutputStream linksOut_outStream, OutputStream linksIn_outStream) {
				this.linksOut_outStream = linksOut_outStream ; 
				this.linksIn_outStream = linksIn_outStream ;
			}
			
			public synchronized void write(ExLinkKey key, DbLinkLocationList value) throws IOException {
				
				ArrayList<DbLinkLocation> ll = value.getLinkLocations() ;
				Collections.sort(ll) ;
				
				DbLinkLocationList sortedValue = new DbLinkLocationList(ll) ;
				
				OutputStream stream ;
				if (key.getIsOut())
					stream = linksOut_outStream ;
				else
					stream = linksIn_outStream ;
				
				CsvRecordOutput csvOutput = new CsvRecordOutput(stream);			
				csvOutput.writeInt(key.getId(), null) ;
				sortedValue.serialize(csvOutput) ;
			}
		
			public synchronized void close(Reporter reporter) throws IOException {
				linksOut_outStream.close();
				linksIn_outStream.close() ;
			}
		}
	}
	
}