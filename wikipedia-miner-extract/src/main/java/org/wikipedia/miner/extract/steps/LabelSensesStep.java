package org.wikipedia.miner.extract.steps;


import gnu.trove.map.hash.TIntIntHashMap;
import gnu.trove.map.hash.TObjectIntHashMap;

import java.io.File;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.* ;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opennlp.maxent.io.SuffixSensitiveGISModelReader;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.Span;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
import org.apache.hadoop.mapred.lib.MultipleOutputs;
import org.apache.hadoop.record.CsvRecordOutput;
import org.apache.hadoop.record.Record;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.log4j.Logger;

import org.wikipedia.miner.model.Page.PageType;
import org.wikipedia.miner.db.struct.DbLinkLocation;
import org.wikipedia.miner.db.struct.DbSentenceSplitList;
import org.wikipedia.miner.db.struct.DbTranslations;
import org.wikipedia.miner.extract.DumpExtractor;
import org.wikipedia.miner.extract.DumpExtractor.ExtractionStep;
import org.wikipedia.miner.extract.model.DumpLink;
import org.wikipedia.miner.extract.model.DumpLinkParser;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.model.struct.*;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.extract.util.Util;
import org.wikipedia.miner.extract.util.XmlInputFormat;
import org.wikipedia.miner.util.MarkupStripper;


//TODO doc out of date
/**
 * The third step in the extraction process.
 * 
 * This produces the following sequence files (in <i>&lt;ouput_dir&gt;/step3/</i>)
 * <ul>
 * <li><b>tempLabel-xxxxx</b> - associates label text (String) with label (ExLabel) - missing term and doc counts.</li>
 * <li><b>tempPageLink-xxxxx</b> - lists source/target pairs for page links with pages represented by Integer ids and redirects bypassed.</li>
 * <li><b>tempCategoryParent-xxxxx</b> - lists child category/parent category pairs for category links with pages represented by Integer ids.</li>
 * <li><b>tempArticleParent-xxxxx</b> - lists child article/parent category pairs for category links with pages represented by Integer ids.</li>
 * <li><b>translations-xxxxx</b> - associates page id (Integer) with a map of translations by language code </li>
 * 
 * </ul>
 */
public class LabelSensesStep extends Configured implements Tool {

	public enum Output {tempLabel, tempPageLink, tempCategoryParent, tempArticleParent, sentenceSplits, translations, fatalErrors} ;


	public int run(String[] args) throws Exception {

		JobConf conf = new JobConf(LabelSensesStep.class);
		DumpExtractor.configureJob(conf, args) ;

		conf.setJobName("WM: gather label senses");

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(ExLabel.class);

		conf.setMapperClass(LabelSensesMapper.class);
		conf.setCombinerClass(LabelSensesReducer.class) ;
		conf.setReducerClass(LabelSensesReducer.class) ;

		// set up input

		conf.setInputFormat(XmlInputFormat.class);
		conf.set(XmlInputFormat.START_TAG_KEY, "<page>") ;
		conf.set(XmlInputFormat.END_TAG_KEY, "</page>") ;
		FileInputFormat.setInputPaths(conf, conf.get(DumpExtractor.KEY_INPUT_FILE));

		//set up output

		conf.setOutputFormat(LabelOutputFormat.class);
		FileOutputFormat.setOutputPath(conf, new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) +"/" + DumpExtractor.getDirectoryName(ExtractionStep.labelSense)));

		//set up distributed cache

		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.OUTPUT_SITEINFO).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_LANG_FILE)).toUri(), conf);
		DistributedCache.addCacheFile(new Path(conf.get(DumpExtractor.KEY_SENTENCE_MODEL)).toUri(), conf);


		//cache page files created in 1st step, so we can look up pages by title
		Path pageStepPath = new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.page)) ;
		for (FileStatus fs:FileSystem.get(conf).listStatus(pageStepPath)) {

			if (fs.getPath().getName().startsWith(PageStep.Output.tempPage.name())) {
				Logger.getLogger(LabelSensesStep.class).info("Cached page file " + fs.getPath()) ;
				DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
			}
		}

		//cache redirect files created in 2nd step, so we can look up pages by title
		Path redirectStepPath = new Path(conf.get(DumpExtractor.KEY_OUTPUT_DIR) + "/" + DumpExtractor.getDirectoryName(ExtractionStep.redirect)) ;
		for (FileStatus fs:FileSystem.get(conf).listStatus(redirectStepPath)) {

			if (fs.getPath().getName().startsWith(RedirectStep.Output.redirectTargetsBySource.name())) {
				Logger.getLogger(LabelSensesStep.class).info("Cached redirect file " + fs.getPath()) ;
				DistributedCache.addCacheFile(fs.getPath().toUri(), conf);
			}
		}

		MultipleOutputs.addNamedOutput(conf, Output.tempPageLink.name(), IntRecordOutputFormat.class,
				IntWritable.class, DbLinkLocation.class);

		MultipleOutputs.addNamedOutput(conf, Output.tempCategoryParent.name(), TextOutputFormat.class,
				IntWritable.class, IntWritable.class);

		MultipleOutputs.addNamedOutput(conf, Output.tempArticleParent.name(), TextOutputFormat.class,
				IntWritable.class, IntWritable.class);

		MultipleOutputs.addNamedOutput(conf, Output.sentenceSplits.name(), IntRecordOutputFormat.class,
				IntWritable.class, DbSentenceSplitList.class);

		MultipleOutputs.addNamedOutput(conf, Output.translations.name(), IntRecordOutputFormat.class,
				IntWritable.class, DbTranslations.class);

		MultipleOutputs.addNamedOutput(conf, Output.fatalErrors.name(), TextOutputFormat.class,
				IntWritable.class, Text.class);



		conf.set("mapred.textoutputformat.separator", ",");

		JobClient.runJob(conf);
		return 0;
	}


	/**
	 *	Takes xml markup of pages (one page element per record) and emits 
	 *		-key: redirect id
	 *		-value: redirect target id
	 */
	private static class LabelSensesMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, ExLabel> {

		private LanguageConfiguration lc ;
		private SiteInfo si ;

		private DumpPageParser pageParser ;
		private DumpLinkParser linkParser ;

		Vector<Path> pageFiles = new Vector<Path>() ;
		private TObjectIntHashMap<String> articlesByTitle = null ; 
		private TObjectIntHashMap<String> categoriesByTitle = null ; 

		Vector<Path> redirectFiles = new Vector<Path>() ;
		private TIntIntHashMap redirectTargetsBySource = null ; 

		private MarkupStripper stripper = new MarkupStripper() ;
		private SentenceDetectorME sentenceDetector ;

		private MultipleOutputs mos ;

		Pattern paragraphSplitPattern = Pattern.compile("\n(\\s*)[\n\\:\\*\\#]") ;




		@Override
		public void configure(JobConf job) {





			try {


				for (Path p:DistributedCache.getLocalCacheFiles(job)) {
					Logger.getLogger(LabelSensesMapper.class).info("Located cached file " + p.toString()) ;

					Logger.getLogger(LabelSensesMapper.class).info(p.getName() + " v.s " + new Path(job.get(DumpExtractor.KEY_SENTENCE_MODEL)).getName()) ;

					if (p.getName().equals(new Path(job.get(DumpExtractor.KEY_SENTENCE_MODEL)).getName())) {
						Logger.getLogger(LabelSensesMapper.class).info("Located cached sentence model " + p.toString()) ;
						File sentenceModelFile = new File(p.toString()) ;
						
						InputStream sentenceModelStream = new FileInputStream(sentenceModelFile);
						SentenceModel model = null ;
						try {
						  model = new SentenceModel(sentenceModelStream);
						}
						catch (IOException e) {
						  e.printStackTrace();
						}
						finally {
						  if (sentenceModelStream != null) {
						    try {
						    	sentenceModelStream.close();
						    }
						    catch (IOException e) {
						    }
						  }
						}

						sentenceDetector = new SentenceDetectorME(model) ;
					}

					if (p.getName().equals(new Path(DumpExtractor.OUTPUT_SITEINFO).getName())) {
						si = new SiteInfo(p) ;
					}

					if (p.getName().equals(new Path(job.get(DumpExtractor.KEY_LANG_FILE)).getName())) {
						lc = new LanguageConfiguration(job.get(DumpExtractor.KEY_LANG_CODE), p) ;
					}

					if (p.getName().startsWith(PageStep.Output.tempPage.name())) {
						Logger.getLogger(LabelSensesMapper.class).info("Located cached page file " + p.toString()) ;
						pageFiles.add(p) ;
					}

					if (p.getName().startsWith(RedirectStep.Output.redirectTargetsBySource.name())) {
						Logger.getLogger(LabelSensesMapper.class).info("Located cached redirect file " + p.toString()) ;
						redirectFiles.add(p) ;
					}
				}

				//for (Path p:DistributedCache.getLocalCacheArchives(job)){
				//	
				//}

				if (si == null) 
					throw new Exception("Could not locate '" + DumpExtractor.OUTPUT_SITEINFO + "' in DistributedCache") ;

				if (lc == null) 
					throw new Exception("Could not locate '" + job.get(DumpExtractor.KEY_LANG_FILE) + "' in DistributedCache") ;

				if (sentenceDetector == null) 
					throw new Exception("Could not load sentence model '" + job.get(DumpExtractor.KEY_SENTENCE_MODEL) + "' from DistributedCache") ;


				if (pageFiles.isEmpty())
					throw new Exception("Could not gather page summary files produced in step 1") ;

				if (redirectFiles.isEmpty())
					throw new Exception("Could not gather redirect summary files produced in step 2") ;

				pageParser = new DumpPageParser(lc, si) ;
				linkParser = new DumpLinkParser(lc, si) ;

				mos = new MultipleOutputs(job);

			} catch (Exception e) {
				Logger.getLogger(LabelSensesMapper.class).error("Could not configure mapper", e);
				System.exit(1) ;
			}
		}

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, ExLabel> output, Reporter reporter) throws IOException {

			DumpPage page = null ;

			try {				
				//set up articlesByTitle and categoriesByTitle, if this hasn't been done already
				//this is done during map rather than configure, so that we can report progress
				//and stop hadoop from declaring a timeout.
				if (articlesByTitle == null || categoriesByTitle == null) {

					HashSet<PageType> articleTypesToCache = new HashSet<PageType>() ;
					articleTypesToCache.add(PageType.article) ;
					articleTypesToCache.add(PageType.redirect) ;
					articleTypesToCache.add(PageType.disambiguation) ;

					HashSet<PageType> categoryTypesToCache = new HashSet<PageType>() ;
					categoryTypesToCache.add(PageType.category) ;

					articlesByTitle = new TObjectIntHashMap<String>() ;
					categoriesByTitle = new TObjectIntHashMap<String>() ;

					for (Path p:pageFiles) {
						articlesByTitle = Util.gatherPageIdsByTitle(p, articleTypesToCache, articlesByTitle, reporter) ;
						categoriesByTitle = Util.gatherPageIdsByTitle(p, categoryTypesToCache, categoriesByTitle, reporter) ;
					}
				}


				//same with redirects
				if (redirectTargetsBySource == null) {
					redirectTargetsBySource = new TIntIntHashMap() ;
					for (Path p:redirectFiles) {
						redirectTargetsBySource = Util.gatherRedirectTargetsBySource(p, redirectTargetsBySource, reporter) ;
					}
				}

				page = pageParser.parsePage(value.toString()) ;

				if (page != null) {

					// build up all the anchors locally for this document before emitting, to maintain docCounts and occCounts
					HashMap<String, ExLabel> labels = new HashMap<String, ExLabel>() ;

					//build up all links and locations locally for this document before emitting, so they are sorted and grouped properly (this doesn't go though a reduce phase)
					TreeMap<Integer, ArrayList<Integer>> outLinksAndLocations = new TreeMap<Integer, ArrayList<Integer>>() ;

					//build up translations tree map
					TreeMap<String, String> translationsByLangCode = new TreeMap<String, String>() ;


					ExLabel label ;

					switch(page.getType()) {

					case article :
					case disambiguation :
						// add association from this article title to article.
						label = new ExLabel(0,0,0,0,new TreeMap<Integer, ExSenseForLabel>()) ;
						label.getSensesById().put(page.getId(), new ExSenseForLabel(0, 0, true, false)) ;	
						labels.put(page.getTitle(), label) ;

					case category :

						String markup = page.getMarkup() ;
						markup = stripper.stripAllButInternalLinksAndEmphasis(markup, ' ') ;
						gatherCategoryLinksAndTranslations(page, markup, translationsByLangCode, reporter) ;
						markup = stripper.stripNonArticleInternalLinks(markup, ' ') ;


						TreeSet<Integer> sentenceSplits = collectSentenceSplits(page.getId(), markup, reporter) ;

						int sentenceIndex = 0 ;
						int lastPos = 0 ;
						for (int currPos:sentenceSplits) {
							processSentence(markup.substring(lastPos, currPos), sentenceIndex, page, labels, outLinksAndLocations, reporter) ;

							lastPos = currPos ;
							sentenceIndex ++ ;

							reporter.progress() ;
						}

						processSentence(markup.substring(lastPos), sentenceIndex, page, labels, outLinksAndLocations, reporter) ;

						break ;
					case redirect :

						// add association from this redirect title to target.
						Integer targetId = Util.getTargetId(page.getTarget(), articlesByTitle, redirectTargetsBySource) ;

						if (targetId != null) {
							label = new ExLabel(0,0,0,0,new TreeMap<Integer, ExSenseForLabel>()) ;
							label.getSensesById().put(targetId, new ExSenseForLabel(0, 0, false, true)) ;

							labels.put(page.getTitle(), label) ;
						}
						break ;
					}

					// now emit all of the labels we have gathered
					for (Map.Entry<String,ExLabel> entry:labels.entrySet()) {
						output.collect(new Text(entry.getKey()), entry.getValue());
					}		

					// now emit all of the outlinks (and their locations) that we have gathered

					for (Map.Entry<Integer,ArrayList<Integer>> entry:outLinksAndLocations.entrySet()) {
						mos.getCollector(Output.tempPageLink.name(), reporter).collect(new IntWritable(page.getId()), new DbLinkLocation(entry.getKey(), entry.getValue()));
					}

					// now emit collected translations
					if (!translationsByLangCode.isEmpty())
						mos.getCollector(Output.translations.name(), reporter).collect(new IntWritable(page.getId()), new DbTranslations(translationsByLangCode)) ;


				}



			} catch (Exception e) {
				Logger.getLogger(LabelSensesMapper.class).error("Caught exception", e) ;

				StringWriter sw = new StringWriter() ;
				PrintWriter pw = new PrintWriter(sw) ;

				e.printStackTrace(pw) ;

				if (page != null) {
					mos.getCollector(Output.fatalErrors.name(), reporter).collect(new IntWritable(page.getId()), new Text(sw.toString().replace('\n', ';'))) ;
				} else {
					mos.getCollector(Output.fatalErrors.name(), reporter).collect(new IntWritable(-1), new Text(sw.toString().replace('\n', ';'))) ;
				}

			}
		}



		private TreeSet<Integer> collectSentenceSplits(int pageId, String markup, Reporter reporter) throws IOException {

			TreeSet<Integer> sentenceSplits = new TreeSet<Integer> () ;

			//mask links so that it is impossible to split on any punctuation within a link.
			String markup_linksMasked = stripper.stripRegions(markup, stripper.gatherComplexRegions(markup, "\\[\\[", "\\]\\]"), 'a') ;

			//also mask content in brackets, so it is impossible to split within these. 
			markup_linksMasked = stripper.stripRegions(markup_linksMasked, stripper.gatherComplexRegions(markup_linksMasked, "\\(", "\\)"), 'a') ;

			//add all splits detected by OpenNLP sentenceDetector
			for(Span span:sentenceDetector.sentPosDetect(markup_linksMasked))
				sentenceSplits.add(span.getEnd()) ;

			//add all splits detected in markup (multiple newlines, or lines starting with indent or list marker)

			Matcher m = paragraphSplitPattern.matcher(markup_linksMasked) ;

			int lastPos = 0 ;
			while (m.find()) {
				int pos = m.start() ;

				if (markup_linksMasked.substring(lastPos, pos).trim().length() > 0)
					sentenceSplits.add(pos) ;

				lastPos = pos ;
			}


			// collect sentence splits
			if (sentenceSplits.size() > 0) {
				ArrayList<Integer> ss = new ArrayList<Integer>() ;
				for (int s:sentenceSplits)
					ss.add(s) ;

				mos.getCollector(Output.sentenceSplits.name(), reporter).collect(new IntWritable(pageId), new DbSentenceSplitList(ss));
			}

			return sentenceSplits ;
		}

		private void processSentence(String sentence, int sentenceIndex, DumpPage page, HashMap<String, ExLabel> labels, TreeMap<Integer, ArrayList<Integer>> outLinksAndLocations, Reporter reporter) throws Exception {

			ExLabel label ;

			Vector<int[]> linkRegions = stripper.gatherComplexRegions(sentence, "\\[\\[", "\\]\\]") ;

			for(int[] linkRegion: linkRegions) {

				String linkMarkup = sentence.substring(linkRegion[0]+2, linkRegion[1]-2) ;

				DumpLink link = null ;
				try {
					link = linkParser.parseLink(linkMarkup) ;
				} catch (Exception e) {
					Logger.getLogger(LabelSensesMapper.class).warn("Could not parse link markup '" + linkMarkup + "'") ;
				}

				if (link != null && link.getTargetNamespace()==SiteInfo.MAIN_KEY) {


					Integer targetId = Util.getTargetId(link.getTargetTitle(), articlesByTitle, redirectTargetsBySource) ;

					if (targetId != null) {
						label = labels.get(link.getAnchor()) ;

						if (label == null) {

							label = new ExLabel(1,1,0,0,new TreeMap<Integer, ExSenseForLabel>()) ;
							label.getSensesById().put(targetId, new ExSenseForLabel(1, 1, false, false)) ;
						} else {

							ExSenseForLabel sense = label.getSensesById().get(targetId) ;

							if (sense == null) {
								sense = new ExSenseForLabel(1, 1, false, false) ;											
							} else {
								sense.setLinkDocCount(1) ;
								sense.setLinkOccCount(sense.getLinkOccCount() + 1) ;									
							}

							label.setLinkOccCount(label.getLinkOccCount() + 1) ;
							label.getSensesById().put(targetId, sense) ;
						}

						labels.put(link.getAnchor(), label) ;



						ArrayList<Integer> locations = outLinksAndLocations.get(targetId) ; 

						if (locations == null) 
							locations = new ArrayList<Integer>() ;

						//only add sentence location if it isn't already there. This is sorted, so just check last element.
						if (locations.isEmpty() || locations.get(locations.size()-1) < sentenceIndex) 
							locations.add(sentenceIndex) ;

						outLinksAndLocations.put(targetId, locations) ;

					} else {
						Logger.getLogger(LabelSensesMapper.class).warn("Could not resolve page link '" + link.getTargetTitle() + "'") ;
					}

				}				
			}
		}


		private void gatherCategoryLinksAndTranslations(DumpPage page, String markup, TreeMap<String,String> translationsByLangCode, Reporter reporter) throws Exception {

			Vector<int[]> linkRegions = stripper.gatherComplexRegions(markup, "\\[\\[", "\\]\\]") ;

			for(int[] linkRegion: linkRegions) {
				String linkMarkup = markup.substring(linkRegion[0]+2, linkRegion[1]-2) ;

				DumpLink link = null ;
				try {
					link = linkParser.parseLink(linkMarkup) ;
				} catch (Exception e) {
					Logger.getLogger(LabelSensesMapper.class).warn("Could not parse link markup '" + linkMarkup + "'") ;
				}

				if (link == null)
					continue ;

				if (link.getTargetLanguage() != null) {
					translationsByLangCode.put(link.getTargetLanguage(), link.getAnchor()) ;
					continue ;
				}

				if (link.getTargetNamespace()==SiteInfo.CATEGORY_KEY)  {
					Integer parentId = Util.getTargetId(link.getTargetTitle(), categoriesByTitle, null) ;

					if (parentId != null) {
						if (page.getNamespace() == SiteInfo.CATEGORY_KEY)
							mos.getCollector(Output.tempCategoryParent.name(), reporter).collect(new IntWritable(page.getId()), new IntWritable(parentId));
						else
							mos.getCollector(Output.tempArticleParent.name(), reporter).collect(new IntWritable(page.getId()), new IntWritable(parentId));
					} else {
						Logger.getLogger(LabelSensesMapper.class).warn("Could not resolve category link '" + link.getTargetTitle() + "'") ;
					}
				}
			}
		}

		@Override
		public void close() throws IOException {
			super.close() ;
			mos.close();
		}
	}


	public static class LabelSensesReducer extends MapReduceBase implements Reducer<Text, ExLabel, Text, ExLabel> {

		public void reduce(Text key, Iterator<ExLabel> values, OutputCollector<Text, ExLabel> output, Reporter reporter) throws IOException {

			ExLabel label = new ExLabel(0,0,0,0,new TreeMap<Integer, ExSenseForLabel>()) ;

			while (values.hasNext()) {

				ExLabel currLabel = values.next();

				for (Map.Entry<Integer,ExSenseForLabel> entry:currLabel.getSensesById().entrySet()) {

					ExSenseForLabel newSense = entry.getValue() ;
					ExSenseForLabel existingSense = label.getSensesById().get(entry.getKey()) ;

					if (existingSense == null) {
						existingSense = newSense ;
					} else {
						existingSense.setLinkOccCount(existingSense.getLinkOccCount() + newSense.getLinkOccCount()) ;
						existingSense.setLinkDocCount(existingSense.getLinkDocCount() + newSense.getLinkDocCount()) ;

						if (newSense.getFromRedirect())
							existingSense.setFromRedirect(true) ;

						if (newSense.getFromTitle())
							existingSense.setFromTitle(true) ;
					}

					label.getSensesById().put(entry.getKey(), existingSense) ;
				}

				label.setLinkDocCount(label.getLinkDocCount() + currLabel.getLinkDocCount()) ;
				label.setLinkOccCount(label.getLinkOccCount() + currLabel.getLinkOccCount()) ;
			}	

			output.collect(key, label);
		}
	}

	protected static class IntRecordOutputFormat extends TextOutputFormat<IntWritable, Record> {

		public RecordWriter<IntWritable, Record> getRecordWriter(FileSystem ignored,
				JobConf job,
				String name,
				Progressable progress)
				throws IOException {

			Path file = FileOutputFormat.getTaskOutputPath(job, name);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new IntRecordWriter(fileOut);
		}

		protected static class IntRecordWriter implements RecordWriter<IntWritable, Record> {

			protected DataOutputStream outStream ;

			public IntRecordWriter(DataOutputStream out) {
				this.outStream = out ; 
			}

			public synchronized void write(IntWritable key, Record value) throws IOException {

				CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);

				csvOutput.writeInt(key.get(), null) ;
				value.serialize(csvOutput) ; 
			}

			public synchronized void close(Reporter reporter) throws IOException {
				outStream.close();
			}
		}
	}

	protected static class LabelOutputFormat extends TextOutputFormat<Text, ExLabel> {

		public RecordWriter<Text, ExLabel> getRecordWriter(FileSystem ignored,
				JobConf job,
				String name,
				Progressable progress)
				throws IOException {

			String newName = name.replace("part", Output.tempLabel.name()) ;

			Path file = FileOutputFormat.getTaskOutputPath(job, newName);
			FileSystem fs = file.getFileSystem(job);
			FSDataOutputStream fileOut = fs.create(file, progress);
			return new LabelRecordWriter(fileOut);
		}	

		protected static class LabelRecordWriter implements RecordWriter<Text, ExLabel> {

			protected DataOutputStream outStream ;

			public LabelRecordWriter(DataOutputStream out) {
				this.outStream = out ; 
			}

			public synchronized void write(Text key, ExLabel value) throws IOException {

				CsvRecordOutput csvOutput = new CsvRecordOutput(outStream);

				csvOutput.writeString(key.toString(), "label") ;
				value.serialize(csvOutput) ; 
			}

			public synchronized void close(Reporter reporter) throws IOException {
				outStream.close();
			}
		}
	}
}
