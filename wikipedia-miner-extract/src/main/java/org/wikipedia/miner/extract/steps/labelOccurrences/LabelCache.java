package org.wikipedia.miner.extract.steps.labelOccurrences;


import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import opennlp.tools.tokenize.SimpleTokenizer;
import opennlp.tools.tokenize.Tokenizer;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.Pair;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.struct.LabelSenseList;
import org.wikipedia.miner.util.ProgressTracker;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class LabelCache {

	//this is a singleton, so that it can be retrieved by multiple mappers within the same JVM
	private static Logger logger = Logger.getLogger(LabelCache.class) ;
	
	private static final double falsePositiveProbability = 0.005 ;
	private static final double desiredLabelPopulation = 0.995 ;
	
	private static LabelCache labelCache ;
	
	
	public static LabelCache get() {
		if (labelCache == null)
			labelCache = new LabelCache() ;

		return labelCache ;
	}

	BloomFilter<CharSequence> labels ;
	private int maxLabelLength = 0 ;
	private int maxSensibleLabelLength = 0 ;
	
	List<Integer> lengthHistogram = new ArrayList<Integer>() ;
	
	
	private boolean isLoaded = false ;

	public boolean isLoaded() {
		return isLoaded ;
	}
	
	public int getMaxLabelLength() {
		return maxLabelLength ;
	}
	
	public int getMaxSensibleLabelLength() {
		return maxSensibleLabelLength ;
	}

	public boolean mightContain(CharSequence label) {
		return labels.mightContain(label) ;
	}

	public void load(List<Path> paths, int totalLabels, Reporter reporter) throws IOException {

		if (isLoaded)
			return ;
		
		logger.info("Caching " + totalLabels + " labels");
		
		Runtime r = Runtime.getRuntime() ;
		long memBefore = r.totalMemory() ;
		
		labels = BloomFilter.create(Funnels.unencodedCharsFunnel(),totalLabels, falsePositiveProbability) ;
		int labelsInserted = 0 ;
		
		
		
		
		ProgressTracker tracker = new ProgressTracker(totalLabels, "Loading labels", getClass()) ;
		
		

		for (Path path:paths) {
			
			logger.info("Caching labels from " + path);
			
			tracker.update();
			
			File file = new File(path.toString()) ;
			
			Schema schema = Pair.getPairSchema(Schema.create(Type.STRING),LabelSenseList.getClassSchema()) ;
			
			DatumReader<Pair<CharSequence,LabelSenseList>> datumReader = new SpecificDatumReader<Pair<CharSequence,LabelSenseList>>(schema);

			FileReader<Pair<CharSequence,LabelSenseList>> fileReader = DataFileReader.openReader(file, datumReader) ;

			while (fileReader.hasNext()) {

				Pair<CharSequence,LabelSenseList> pair = fileReader.next();
				
				
				
				CharSequence label = pair.key() ;
				labels.put(label) ;
				labelsInserted++ ;
				updateLengthHistogram(label) ;
								
				reporter.progress() ;

			}

			fileReader.close() ;
		}
		
		long memAfter = r.totalMemory() ;
		logger.info("Memory Used: " + (memAfter - memBefore) / (1024*1024) + "Mb") ;
		
		isLoaded = true ;
		calculateMaximumSensibleLength(totalLabels) ;
		
		logger.info("Longest label: " +  maxLabelLength);
		logger.info("Longest sensible label: " +  maxSensibleLabelLength);
		logger.info("labels expected: " + totalLabels + ", labels inserted: " + labelsInserted);
		printLengthHistogram() ;
	}
	
	private void updateLengthHistogram(CharSequence label) {
		
		Tokenizer tokenizer = SimpleTokenizer.INSTANCE ;
		String[] tokens = tokenizer.tokenize(label.toString()) ;
		
		if (maxLabelLength < tokens.length)
			maxLabelLength = tokens.length ;
		
		while (lengthHistogram.size() <= tokens.length)
			lengthHistogram.add(0) ;
		
		lengthHistogram.set(tokens.length, lengthHistogram.get(tokens.length)+1) ;
		
		if (tokens.length > 20)
			logger.info(" - " + label);
		
	}
	
	private void calculateMaximumSensibleLength(int totalLabels) {
		
		//we can have a few tokens that are extremely long, which will cause the mapper to be very slow. 
		//we will ignore tokens that are longer than 99% of the population
		
		int total = 0 ;
		
		int length ;
		for (length = 0; length<lengthHistogram.size(); length++) {
			
			total = total + lengthHistogram.get(length) ;
			
			double popProportion = (double)total/totalLabels ;
			
			if (popProportion > desiredLabelPopulation)
				break ;
		}
		
		maxSensibleLabelLength = length ;		
	}
	
	private void printLengthHistogram() {
		
		for (int length=0 ; length<lengthHistogram.size(); length++) 
			logger.info(lengthHistogram.get(length) + " labels with " + length + " tokens");
	}
	
}
