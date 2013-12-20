package org.wikipedia.miner.extract;

import static org.junit.Assert.assertNotNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLStreamException;

import org.junit.Before;
import org.junit.Test;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.core.Persister;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.model.DumpPageParser;
import org.wikipedia.miner.extract.util.Languages;
import org.wikipedia.miner.extract.util.Languages.Language;
import org.wikipedia.miner.extract.util.SiteInfo;
import org.wikipedia.miner.util.MarkupStripper;

public class MarkupTestCase {

	private SiteInfo siteInfo ;
	private Language langConf ;
	private MarkupStripper stripper = new MarkupStripper() ;
		
	@Before
	public void init() throws FactoryConfigurationError, Exception {
		siteInfo = loadSiteInfo() ;
		langConf = loadLanguageConfig("simple") ;
	}
	
	@Test
	public void test() {
		
		assertNotNull(langConf) ;
		
	}
	
	private SiteInfo loadSiteInfo() throws Exception {

		ClassLoader classloader = Thread.currentThread().getContextClassLoader() ;
		return SiteInfo.load(classloader.getResourceAsStream("siteInfo.xml")) ;
	}
	
	private Language loadLanguageConfig(String langCode) throws Exception {

		Serializer serializer = new Persister();
		File source = new File("../configs/languages.xml");

		Languages languages = serializer.read(Languages.class, source);
		
		return languages.get(langCode) ;
	}
	
	public DumpPage loadPage(String fileName) throws IOException, XMLStreamException {

		DumpPageParser parser = new DumpPageParser(langConf, siteInfo) ;

		ClassLoader classloader = Thread.currentThread().getContextClassLoader() ;
		BufferedReader reader = new BufferedReader(new InputStreamReader(classloader.getResourceAsStream(fileName)));

		StringBuffer sb = new StringBuffer() ;

		String line ;
		while ((line = reader.readLine()) != null) {
			sb.append(line) ;
			sb.append("\n") ;

		}

		return parser.parsePage(sb.toString()) ;

	}

	public SiteInfo getSiteInfo() {
		return siteInfo;
	}

	public Language getLangConf() {
		return langConf;
	}

	public MarkupStripper getStripper() {
		return stripper;
	}
	
	
}
