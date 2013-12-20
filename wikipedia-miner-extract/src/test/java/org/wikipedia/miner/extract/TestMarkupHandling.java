package org.wikipedia.miner.extract;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import opennlp.tools.util.InvalidFormatException;

import org.junit.Before;
import org.junit.Test;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.util.PageSentenceExtractor;

public class TestMarkupHandling extends MarkupTestCase {


	private PageSentenceExtractor sentenceExtractor ;
	

	@Test
	public void testParsing() throws XMLStreamException, FactoryConfigurationError, IOException {
		
		
		
		DumpPage page = loadPage("autonomousCommunitiesOfSpain.xml");
		
		assertEquals(page.getId(), 12) ;
		assertEquals(page.getTitle(), "Autonomous communities of Spain") ;
		assertEquals(page.getNamespace().getKey(), 0) ;
		
	}
	
	
	@Test
	public void testSentenceExtraction() throws XMLStreamException, IOException {
		
				
		DumpPage page = loadPage("autonomousCommunitiesOfSpain.xml");
		
		String markup = page.getMarkup() ;
		
		String strippedMarkup = getStripper().stripAllButInternalLinksAndEmphasis(markup, ' ') ;
		
		assertEquals(markup.length(), strippedMarkup.length()) ;
		
		List<Integer> sentenceSplits = sentenceExtractor.getSentenceSplits(page) ;
		//System.out.println(StringUtils.join(sentenceSplits, ",")) ;
		
		assertEquals(sentenceSplits.size(), 34) ;
		
		/*
		int lastSplit = 0 ;
		for (int split : sentenceSplits) {
			
			System.out.println("s: " + markup.substring(lastSplit, split)) ;
			
			lastSplit = split ;
		}
		*/
	}

	
	@Before
	public void init() throws FactoryConfigurationError, Exception {
		super.init();
		
		sentenceExtractor = loadSentenceExtractor() ;
	}

	
	private PageSentenceExtractor loadSentenceExtractor() throws InvalidFormatException, IOException {

		File sentenceModelFile = new File("../models/en-sent.bin") ;

		return new PageSentenceExtractor(sentenceModelFile) ;


	}
 

}
