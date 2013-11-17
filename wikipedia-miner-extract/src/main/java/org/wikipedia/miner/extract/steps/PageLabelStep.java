package org.wikipedia.miner.extract.steps;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
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
import org.wikipedia.miner.db.struct.DbLabel;
import org.wikipedia.miner.db.struct.DbLabelForPage;
import org.wikipedia.miner.db.struct.DbLabelForPageList;

import org.wikipedia.miner.db.struct.DbSenseForLabel;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.DumpExtractor.ExtractionStep;
import org.wikipedia.miner.extract.model.struct.ExLabel;
import org.wikipedia.miner.extract.model.struct.ExLinkKey;
import org.wikipedia.miner.extract.model.struct.ExSenseForLabel;

public class PageLabelStep extends Configured implements Tool {

	
	public enum Output {pageLabel} ;
	
	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(CategoryLinkSummaryStep.class);
		DumpExtractor.configureJob(conf, args) ;
		
		conf.setJobName("WM: summarize page labels") ;
		
		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(DbLabelForPageList.class);

		conf.setMapperClass(PageLabelMapper.class);
		conf.setCombinerClass(PageLabelReducer.class) ;
		conf.setReducerClass(PageLabelReducer.class) ;

		// set up input

		conf.setInputFormat(TextInputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.labelSense) + "/" + LabelSensesStep.Output.tempLabel.name() + "*"));
				
		//set up output

		conf.setOutputFormat(PageLabelOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.pageLabel)));

		JobClient.runJob(conf);
		return 0;
	}
	
	private static class PageLabelMapper extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DbLabelForPageList> {

		@Override
		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DbLabelForPageList> output, Reporter reporter) throws IOException {
			
			CsvRecordInput cri = new CsvRecordInput(new ByteArrayInputStream((value.toString() + "\n").getBytes("UTF-8"))) ;

			String text = cri.readString(null) ;
			ExLabel exlabel = new ExLabel() ;
			exlabel.deserialize(cri) ;
			
			DbLabel dbLabel= DumpExtractor.convert(exlabel) ;
			
			if (dbLabel.getSenses() != null && !dbLabel.getSenses().isEmpty()) {
								
				boolean isPrimary = true ;
				
				for (DbSenseForLabel sense:dbLabel.getSenses()) {
					
					DbLabelForPage pageLabel = new DbLabelForPage(text, sense.getLinkOccCount(), sense.getLinkDocCount(), sense.getFromTitle(), sense.getFromRedirect(), isPrimary) ;
					
					ArrayList<DbLabelForPage> labels = new ArrayList<DbLabelForPage>() ;
					labels.add(pageLabel) ;
					
					output.collect(new IntWritable(sense.getId()), new DbLabelForPageList(labels)) ;
					
					isPrimary = false ;
				}
			}
		}
	}
	
	public static class PageLabelReducer extends MapReduceBase implements Reducer<IntWritable, DbLabelForPageList, IntWritable, DbLabelForPageList> {

		public void reduce(IntWritable key, Iterator<DbLabelForPageList> values, OutputCollector<IntWritable, DbLabelForPageList> output, Reporter reporter) throws IOException {

			ArrayList<DbLabelForPage> collectedValues = new ArrayList<DbLabelForPage>() ;
	
			while (values.hasNext()) {
				DbLabelForPageList v = values.next() ;
				collectedValues.addAll(v.getLabels()) ;
			}

			output.collect(key, new DbLabelForPageList(collectedValues));
		}
	}
	
	public static class PageLabelOutputFormat extends TextOutputFormat<IntWritable, DbLabelForPageList> {

		public RecordWriter<IntWritable, DbLabelForPageList> getRecordWriter(FileSystem ignored,
				JobConf job,
				String name,
				Progressable progress)
				throws IOException {
			
			String newName = name.replace("part", Output.pageLabel.name()) ;
					 
			Path file = FileOutputFormat.getTaskOutputPath(job, newName);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream stream = fs.create(file, progress);
			
			return new PageLabelRecordWriter(stream);
		}	
		
		public static class PageLabelRecordWriter implements RecordWriter<IntWritable, DbLabelForPageList> {

			public OutputStream outStream ;
			
			public PageLabelRecordWriter(OutputStream outStream) {
				this.outStream = outStream ; 
			}
			
			public synchronized void write(IntWritable key, DbLabelForPageList value) throws IOException {
				
				ArrayList<DbLabelForPage> ll = value.getLabels() ;
				Collections.sort(ll, new Comparator<DbLabelForPage>() {

					public int compare(DbLabelForPage a, DbLabelForPage b) {

						int cmp = new Long(b.getLinkOccCount()).compareTo(a.getLinkOccCount()) ;
						if (cmp != 0)
							return cmp ;

						cmp = new Long(b.getLinkDocCount()).compareTo(a.getLinkDocCount()) ;
						if (cmp != 0)
							return cmp ;
						
						cmp = new Boolean(b.getFromTitle()).compareTo(a.getFromTitle()) ;
						if (cmp != 0)
							return cmp ;
						
						cmp = new Boolean(b.getFromRedirect()).compareTo(a.getFromRedirect()) ;
						if (cmp != 0)
							return cmp ;
						
						cmp = new Boolean(b.getIsPrimary()).compareTo(a.getIsPrimary()) ;
						if (cmp != 0)
							return cmp ;
						
						return(a.getText()).compareTo(b.getText()) ;
					}
				}) ;
				
				DbLabelForPageList sortedValue = new DbLabelForPageList(ll) ;
								
				CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);			
				csvOutput.writeInt(key.get(), null) ;
				sortedValue.serialize(csvOutput) ;
			}
		
			public synchronized void close(Reporter reporter) throws IOException {
				outStream.close();
			}
		}
	}
	
	
}
