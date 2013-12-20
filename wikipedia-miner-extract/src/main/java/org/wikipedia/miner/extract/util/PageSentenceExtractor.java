package org.wikipedia.miner.extract.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;
import opennlp.tools.util.Span;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.util.MarkupStripper;

public class PageSentenceExtractor {
	
	private static Logger logger = Logger.getLogger(PageSentenceExtractor.class) ;

	private SentenceDetectorME sentenceDetector ;

	private MarkupStripper stripper = new MarkupStripper() ;

	
	//split paragraphs either after multiple new lines, or on a line that starts with a indent (:) or list(*,#) marker
	private Pattern paragraphSplitPattern = Pattern.compile("((\n\\s*){2,}|\n\\s*[*:#])") ;

	public PageSentenceExtractor(Path sentenceModel) throws InvalidFormatException, IOException {

		InputStream sentenceModelStream = new FileInputStream(sentenceModel.toString());
		init(sentenceModelStream) ;
	}
	
	public PageSentenceExtractor(File sentenceModel) throws InvalidFormatException, IOException {
		
		InputStream sentenceModelStream = new FileInputStream(sentenceModel);
		init(sentenceModelStream) ;
	}


	private void init(InputStream sentenceModelStream) throws InvalidFormatException, IOException {

		SentenceModel model = null ;
		try {
			model = new SentenceModel(sentenceModelStream);
		}
		finally {
			if (sentenceModelStream != null) {
				try {
					sentenceModelStream.close();
				} catch (IOException e) {
					logger.error("Could not close sentence model reader", e);
				}
			}
		}

		sentenceDetector =  new SentenceDetectorME(model) ;


	}


	public List<Integer> getSentenceSplits(DumpPage page) {

	
		String maskedMarkup = stripper.stripAllButInternalLinksAndEmphasis(page.getMarkup(), ' ') ;
		maskedMarkup = stripper.stripNonArticleInternalLinks(maskedMarkup, 'a') ;

		//mask links so that it is impossible to split on any punctuation within a link.
		maskedMarkup = stripper.stripRegions(maskedMarkup, stripper.gatherComplexRegions(maskedMarkup, "\\[\\[", "\\]\\]"), 'a') ;

		return getSentenceSplits(maskedMarkup) ;
		
	}
	
	public List<Integer> getSentenceSplits(String strippedMarkup) {
		
		List<Integer> sentenceSplits = new ArrayList<Integer>() ;
		
		//also mask content in brackets, so it is impossible to split within these. 
		String maskedMarkup = stripper.stripRegions(strippedMarkup, stripper.gatherComplexRegions(strippedMarkup, "\\(", "\\)"), 'a') ;
		
		Matcher paragraphMatcher = paragraphSplitPattern.matcher(maskedMarkup) ;

		int lastParagraphEnd = 0;
		while (paragraphMatcher.find()) {

			int paragraphEnd = paragraphMatcher.start();
			
			String paragraph = maskedMarkup.substring(lastParagraphEnd, paragraphEnd) ;
			
			if (paragraph.trim().length() == 0) 
				continue ;
			
			sentenceSplits = handleParagraph(paragraph, lastParagraphEnd, sentenceSplits) ;
			
			lastParagraphEnd = paragraphMatcher.end()  ;
		}
		
		sentenceSplits = handleParagraph(maskedMarkup.substring(lastParagraphEnd), lastParagraphEnd, sentenceSplits) ;
		
		return sentenceSplits ;
				
	}
	
	public List<Integer> handleParagraph(String paragraph, int paragraphStart, List<Integer> sentenceSplits) {
	
		if (paragraphStart > 0)
			sentenceSplits.add(paragraphStart) ;
		
		Span[] spans = sentenceDetector.sentPosDetect(paragraph) ;
		
		for (int spanIndex = 0 ; spanIndex < spans.length - 1 ; spanIndex++) {
			//add splits for all spans except for the last one (that split gets handled at the paragraph level
			Span span = spans[spanIndex] ;
			
			sentenceSplits.add(paragraphStart + span.getEnd()) ;
			//System.out.println(" - " + (span.getStart() + paragraphStart) + "," + (span.getEnd() + paragraphStart));
		}
		return sentenceSplits ;
	}
	
	
}
