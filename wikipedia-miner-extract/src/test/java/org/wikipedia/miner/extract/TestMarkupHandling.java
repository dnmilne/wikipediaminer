package org.wikipedia.miner.extract;

import static org.junit.Assert.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import opennlp.tools.util.InvalidFormatException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.util.LanguageConfiguration;
import org.wikipedia.miner.extract.util.PageSentenceExtractor;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.util.MarkupStripper;

public class TestMarkupHandling {

	private MarkupStripper stripper = new MarkupStripper() ;
	
	private PageSentenceExtractor sentenceExtractor ;
	
	private SiteInfo si ;
	
	private LanguageConfiguration lc ;
	
	

	@Test
	public void testParsing() throws XMLStreamException, FactoryConfigurationError, IOException {
		
		
		
		DumpPage page = loadPage(lc, si);
		
		assertEquals(page.getId(), 12) ;
		assertEquals(page.getTitle(), "Autonomous communities of Spain") ;
		assertEquals(page.getNamespace(), 0) ;
		
	}
	
	
	@Test
	public void testSentenceExtraction() throws XMLStreamException, IOException {
		
				
		DumpPage page = loadPage(lc, si);
		
		String markup = page.getMarkup() ;
		
		String strippedMarkup = stripper.stripAllButInternalLinksAndEmphasis(markup, ' ') ;
		
		assertEquals(markup.length(), strippedMarkup.length()) ;
		
		List<Integer> sentenceSplits = sentenceExtractor.getSentenceSplits(page) ;
		System.out.println(StringUtils.join(sentenceSplits, ",")) ;
		
		
		int lastSplit = 0 ;
		for (int split : sentenceSplits) {
			
			System.out.println("s: " + markup.substring(lastSplit, split)) ;
			
			lastSplit = split ;
		}
	}

	
	@Before
	public void init() throws XMLStreamException, FactoryConfigurationError, IOException {
		si = loadSiteInfo() ;
		lc = loadLanguageConfig("simple") ;
		
		sentenceExtractor = loadSentenceExtractor() ;
	}
	


	private SiteInfo loadSiteInfo() throws FileNotFoundException, XMLStreamException {

		ClassLoader classloader = Thread.currentThread().getContextClassLoader() ;
		InputStreamReader reader =new InputStreamReader(classloader.getResourceAsStream("siteInfo.xml"));

		return new SiteInfo(reader) ;
	}

	private LanguageConfiguration loadLanguageConfig(String langCode) throws XMLStreamException, FactoryConfigurationError, IOException {

		File langConfFile = new File("../configs/languages.xml") ;

		return new LanguageConfiguration(langCode, langConfFile) ;


	}

	private DumpPage loadPage(LanguageConfiguration lc, SiteInfo si) throws IOException, XMLStreamException {

		DumpPageParser parser = new DumpPageParser(lc, si) ;

		ClassLoader classloader = Thread.currentThread().getContextClassLoader() ;
		BufferedReader reader = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream("page.xml")));

		StringBuffer sb = new StringBuffer() ;

		String line ;
		while ((line = reader.readLine()) != null) {
			sb.append(line) ;
			sb.append("\n") ;

		}

		return parser.parsePage(sb.toString()) ;

	}
	
	private PageSentenceExtractor loadSentenceExtractor() throws InvalidFormatException, IOException {

		File sentenceModelFile = new File("../models/en-sent.bin") ;

		return new PageSentenceExtractor(sentenceModelFile) ;


	}
 

}
