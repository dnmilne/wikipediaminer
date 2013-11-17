package org.wikipedia.miner.extract.util;

import java.util.HashMap ;

import java.io.FileNotFoundException;
import java.io.FileReader;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.fs.Path;

public class SiteInfo {

	private String siteName ;
	private String base ;
	private String generator ;
	private String caseRule ;
	
	private HashMap<String, Integer> namespaceKeysByNamespace = new HashMap<String, Integer>() ;
	
	public static final int MAIN_KEY = 0 ;
	public static final int IMAGE_KEY = 6 ;
	public static final int TEMPLATE_KEY = 10 ;
	public static final int CATEGORY_KEY = 14 ;
	
	public SiteInfo(Path siteInfoFile) throws XMLStreamException, FileNotFoundException {
		
		XMLStreamReader xmlStreamReader = XMLInputFactory.newInstance().createXMLStreamReader(new FileReader(siteInfoFile.toString())) ;
	
		Integer currNamespaceKey = null ;
		StringBuffer characters = new StringBuffer() ;
	
		while (xmlStreamReader.hasNext()) {
	
			int eventCode = xmlStreamReader.next();
	
			switch (eventCode) {
	
			case XMLStreamReader.START_ELEMENT :
					
				if (xmlStreamReader.getLocalName().equals("namespace")) 
					currNamespaceKey = Integer.parseInt(xmlStreamReader.getAttributeValue(null, "key")) ;
				
				characters = new StringBuffer() ;	
				break;
	
			case XMLStreamReader.END_ELEMENT :
				
				if (xmlStreamReader.getLocalName().equals("sitename")) 
					siteName = characters.toString().trim() ;
				
				if (xmlStreamReader.getLocalName().equals("base")) 
					base = characters.toString().trim() ;
				
				if (xmlStreamReader.getLocalName().equals("generator")) 
					generator = characters.toString().trim() ;
				
				if (xmlStreamReader.getLocalName().equals("caseRule")) 
					caseRule = characters.toString().trim() ;
				
				if (xmlStreamReader.getLocalName().equals("namespace")) {
					namespaceKeysByNamespace.put(characters.toString().trim(), currNamespaceKey) ;
					currNamespaceKey = null ;
				}
				
				characters = new StringBuffer() ;
				break;
	
			case XMLStreamReader.CHARACTERS :
				characters.append(xmlStreamReader.getText()) ;
				break ;
			}
		}
	
		xmlStreamReader.close() ;
	
	}

	public String getBase() {
		return base;
	}

	public String getCaseRule() {
		return caseRule;
	}

	public String getGenerator() {
		return generator;
	}

	public HashMap<String, Integer> getNamespaceKeysByNamespace() {
		return namespaceKeysByNamespace;
	}
	
	public Integer getNamespaceKey(String namespace) {
		return namespaceKeysByNamespace.get(namespace) ;
	}

	public String getSiteName() {
		return siteName;
	}
	
}
