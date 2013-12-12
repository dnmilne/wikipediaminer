package org.wikipedia.miner.extract.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Vector;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import java.util.regex.* ;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LanguageConfiguration {
	
	private enum LangTag {Language, RootCategory, DisambiguationCategory, DisambiguationTemplate, RedirectIdentifier, ignorable}

	String rootCategory = null ;  
	Vector<String> disambigCategories = new Vector<String>() ;
	Vector<String> disambigTemplates = new Vector<String>() ;
	Vector<String> redirectIdentifiers = new Vector<String>() ;
	Pattern disambigPattern ;
	Pattern redirectPattern ;
	
	public LanguageConfiguration(FileSystem dfs, String langCode, Path languageConfigFile) throws XMLStreamException, FactoryConfigurationError, IOException {
		this.init(new InputStreamReader(dfs.open(languageConfigFile)), langCode) ;
	}
	
	public LanguageConfiguration(String langCode, Path languageConfigFile) throws XMLStreamException, FactoryConfigurationError, IOException {
		this.init(new FileReader(languageConfigFile.toString()), langCode) ;
	}
	
	public LanguageConfiguration(String langCode, File languageConfigFile) throws XMLStreamException, FactoryConfigurationError, IOException {
		this.init(new FileReader(languageConfigFile), langCode) ;
	}
		
	private void init(InputStreamReader input, String langCode) throws XMLStreamException, FactoryConfigurationError, IOException {
		
		XMLStreamReader xmlStreamReader = XMLInputFactory.newInstance().createXMLStreamReader(input) ;

		boolean relevantLanguage = false ;
		boolean done = false ;
		
		StringBuffer characters = new StringBuffer() ;

		while (xmlStreamReader.hasNext()) {
			
			if (done) break ;

			int eventCode = xmlStreamReader.next();

			switch (eventCode) {

			case XMLStreamReader.START_ELEMENT :
				if (resolveLangTag(xmlStreamReader.getLocalName()) == LangTag.Language && xmlStreamReader.getAttributeValue(null, "code").equalsIgnoreCase(langCode))
					relevantLanguage = true ;
				break;

			case XMLStreamReader.END_ELEMENT :
				if (relevantLanguage) {
					switch(resolveLangTag(xmlStreamReader.getLocalName())) {
					case Language:
						if (relevantLanguage)
							done = true ; 
						break ;
					case RootCategory:
						if (rootCategory != null) 
							throw new XMLStreamException("language configuration specifies multiple root categories") ;
						else 
							rootCategory = characters.toString().trim() ;
						break ;
					case DisambiguationCategory:
						disambigCategories.add(characters.toString().trim());
						break ;
					case DisambiguationTemplate:
						disambigTemplates.add(characters.toString().trim());
						break ;
					case RedirectIdentifier:
						redirectIdentifiers.add(characters.toString().trim());
						break ;
					}
				}
				characters = new StringBuffer() ;
				break;

			case XMLStreamReader.CHARACTERS :
				characters.append(xmlStreamReader.getText()) ;
				break ;
			}
		}

		xmlStreamReader.close() ;
		
		if (!relevantLanguage)
			throw new XMLStreamException("language configuration does not specify variables for language code '" + langCode + "'") ;
		
		if (rootCategory == null) 
			throw new XMLStreamException("language configuration does not specify a root category") ;
		
		if (redirectIdentifiers == null) 
			throw new XMLStreamException("language configuration does not specify any redirect identifiers") ;
		
		//now construct regex pattern for detecting disambig pages ;
		
		String disambigCategoryRegex = null ;
		if (!disambigCategories.isEmpty()) {
			
			StringBuffer tmp = new StringBuffer() ;
			
			tmp.append("\\[\\[\\s*") ;
			
			if (disambigCategories.size() == 1) {
				tmp.append(disambigCategories.firstElement()) ;
			} else {
				tmp.append("(") ;
				for (String dc:disambigCategories) {
					tmp.append(dc) ;
					tmp.append("|") ;
				}
				tmp.deleteCharAt(tmp.length()-1) ;
				tmp.append(")") ;
			}
			tmp.append("\\s*\\]\\]") ;
			
			disambigCategoryRegex = tmp.toString() ;
		}
		
		String disambigTemplateRegex = null ;
		if (!disambigTemplates.isEmpty()) {
			
			StringBuffer tmp = new StringBuffer() ;
			
			tmp.append("\\{\\{\\s*") ;
			
			if (disambigTemplates.size() == 1) {
				tmp.append(disambigTemplates.firstElement()) ;
			} else {
				tmp.append("(") ;
				for (String dt:disambigTemplates) {
					tmp.append(dt) ;
					tmp.append("|") ;
				}
				tmp.deleteCharAt(tmp.length()-1) ;
				tmp.append(")") ;
			}
			tmp.append("\\s*\\}\\}") ;
			
			disambigTemplateRegex = tmp.toString() ;
		}
		
		
		if (disambigCategoryRegex == null && disambigTemplateRegex == null) {
			throw new XMLStreamException("language configuration does not specify any categories or templates for identifying disambiguation pages") ;
		}
		
		if (disambigCategoryRegex != null && disambigTemplateRegex != null) {
			disambigPattern = Pattern.compile("(" + disambigCategoryRegex + "|" + disambigTemplateRegex + ")", Pattern.CASE_INSENSITIVE) ;
		} else {
			if (disambigCategoryRegex != null)
				disambigPattern = Pattern.compile(disambigCategoryRegex, Pattern.CASE_INSENSITIVE) ;
			else
				disambigPattern = Pattern.compile(disambigTemplateRegex, Pattern.CASE_INSENSITIVE) ;
		}
		
		StringBuffer redirectRegex = new StringBuffer("\\#") ;
		redirectRegex.append("(") ;
		for (String ri:redirectIdentifiers) {
			redirectRegex.append(ri) ;
			redirectRegex.append("|") ;
		}
		redirectRegex.deleteCharAt(redirectRegex.length()-1) ;
		redirectRegex.append(")[:\\s]*(?:\\[\\[(.*)\\]\\]|(.*))") ;
		
		redirectPattern = Pattern.compile(redirectRegex.toString(), Pattern.CASE_INSENSITIVE) ;
		
	}
	
	public String getRootCategoryName() {
		return rootCategory ;
	}
	
	public Vector<String> getDisambiguationCategoryNames() {
		return disambigCategories ;
	}
	
	public Vector<String> getDisambiguationTemplateNames() {
		return disambigTemplates ;
	}
	
	public Vector<String> getRedirectIdentifiers() {
		return redirectIdentifiers ;
	}
	
	public Pattern getDisambiguationPattern() {
		return disambigPattern ;
	}
	
	public Pattern getRedirectPattern() {
		return redirectPattern ;
	}
	
	
	private LangTag resolveLangTag(String tagName) {

		try {
			return LangTag.valueOf(tagName) ;
		} catch (IllegalArgumentException e) {
			return LangTag.ignorable ;
		}
	}
	
	public static void main(String args[]) throws XMLStreamException, FactoryConfigurationError, IOException {
		
		Configuration conf = new Configuration() ;
		
		Path p = new Path("configs/languages.xml") ;
		
		FileSystem fs = FileSystem.getLocal(conf) ;
		
		LanguageConfiguration lc = new LanguageConfiguration(fs, "en", p) ;
	}
}
