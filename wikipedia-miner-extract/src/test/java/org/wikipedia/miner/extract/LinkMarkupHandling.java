package org.wikipedia.miner.extract;

import static org.junit.Assert.*;

import java.util.Vector;

import javax.xml.stream.FactoryConfigurationError;

import org.junit.Test;
import org.wikipedia.miner.extract.model.DumpLink;
import org.wikipedia.miner.extract.model.DumpLinkParser;
import org.wikipedia.miner.extract.model.DumpPage;
import org.wikipedia.miner.extract.util.SiteInfo;

public class LinkMarkupHandling extends MarkupTestCase{

	
	
	
	@Test
	public void testLinkParsing() throws FactoryConfigurationError, Exception {
		
		DumpLinkParser linkParser = new DumpLinkParser(getLangConf(), getSiteInfo()) ;
		
		DumpPage page = loadPage("april.xml");
		
		String markup = page.getMarkup() ;
		
		Vector<int[]> linkRegions = getStripper().gatherComplexRegions(markup, "\\[\\[", "\\]\\]") ;

		for(int[] linkRegion: linkRegions) {

			String linkMarkup = markup.substring(linkRegion[0]+2, linkRegion[1]-2) ;
			
			linkParser.parseLink(linkMarkup, page.getTitle()) ;
		
			//System.out.println(linkMarkup) ;
			//System.out.println("ns:" + link.getTargetNamespace() + " anchor:" + link.getAnchor()) ;
		}
		
	}
	
	@Test
	public void testLinkVariants() throws Exception {
		
		DumpLinkParser linkParser = new DumpLinkParser(getLangConf(), getSiteInfo()) ;
		
		DumpLink link = linkParser.parseLink("Cambodia", "Thailand") ;
		assertEquals(link.getTargetNamespace().getKey(), SiteInfo.MAIN_KEY) ;
		assertEquals(link.getTargetTitle(), "Cambodia") ;
		assertEquals(link.getAnchor(), "Cambodia") ;
		
		link = linkParser.parseLink("Cambodia#Population|Cambodia's Population", "Thailand") ;
		assertEquals(link.getTargetNamespace().getKey(), SiteInfo.MAIN_KEY) ;
		assertEquals(link.getTargetTitle(), "Cambodia") ;
		assertEquals(link.getTargetSection(), "Population") ;
		assertEquals(link.getAnchor(), "Cambodia's Population") ;
		
		link = linkParser.parseLink("#Population|Cambodia's Population", "Cambodia") ;
		assertEquals(link.getTargetNamespace().getKey(), SiteInfo.MAIN_KEY) ;
		assertEquals(link.getTargetTitle(), "Cambodia") ;
		assertEquals(link.getTargetSection(), "Population") ;
		assertEquals(link.getAnchor(), "Cambodia's Population") ;
		
		
		link = linkParser.parseLink("Category:Cambodia| ", "Thailand") ;
		assertEquals(link.getTargetNamespace().getKey(), SiteInfo.CATEGORY_KEY) ;
		assertEquals(link.getTargetTitle(), "Cambodia") ;
		assertEquals(link.getAnchor(), "Cambodia") ;
		
		link = linkParser.parseLink("Image:8denarii.jpg|thumb|400px|row 1 : 157 BC [[Roman Republic]], 73 AD [[Vespasian]], 161 AD [[Marcus Aurelius]], 194 AD [[Septimius Severus]];\n" +     
"row 2: 199 AD [[Caracalla]], 200 AD [[Julia Domna]], 219 AD [[Elagabalus]], 236 AD [[Maximinus Thrax]].", "Denarii") ;
		assertEquals(link.getTargetNamespace().getKey(), SiteInfo.FILE_KEY) ;
		assertEquals(link.getTargetTitle(), "8denarii.jpg") ;
		
		link = linkParser.parseLink("Hellenic Parliament-MPs swearing in.png|thumb|left|The Greek [[parliament]] is in [[Athens]].", "Greece") ;
		assertEquals(link.getTargetNamespace().getKey(), SiteInfo.FILE_KEY) ;
		assertEquals(link.getTargetTitle(), "Hellenic Parliament-MPs swearing in.png") ;
		
		
	}
	
	
	
	
}
