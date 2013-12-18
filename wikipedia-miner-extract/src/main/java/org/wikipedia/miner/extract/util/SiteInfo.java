package org.wikipedia.miner.extract.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.Text;
import org.simpleframework.xml.Transient;
import org.simpleframework.xml.core.Persister;

@Root
public class SiteInfo {
	
	public static final int MAIN_KEY = 0 ;
	public static final int SPECIAL_KEY = -1 ;
	public static final int FILE_KEY = 6 ;
	public static final int TEMPLATE_KEY = 10 ;
	public static final int CATEGORY_KEY = 14 ;
	
	@Element(name="sitename")
	private String siteName ;
	
	@Element
	private String base ;
	
	@Element
	private String generator ;
	
	@Element(name="case") 
	private String caseRule ;
	
	@ElementList(name="namespaces",entry="namespace")
	private List<Namespace> namespaces ;
	
	@Transient
	private Map<String, Namespace> namespacesByName  ;
	
	@Transient
	private Map<Integer, Namespace> namespacesByKey ;
	
	public static SiteInfo load(File file) throws Exception {
		
		Serializer serializer = new Persister();
		return serializer.read(SiteInfo.class, file);
		
	}
	
	public static SiteInfo load(InputStream input) throws Exception {
		
		Serializer serializer = new Persister();
		return serializer.read(SiteInfo.class, input) ;
		
	}
	
	public static SiteInfo loadFromDump(File file) throws Exception {
		
		final int maxBeforeLines = 100 ;
		final int maxDuringLines = 100 ;
		
		StringBuffer sb = new StringBuffer() ;
		
		BufferedReader reader = new BufferedReader(new FileReader(file)) ;
		
		boolean started = false ;
		String line ;
		int beforeLineCount = 0 ;
		int duringLineCount = 0 ;
		
		while ((line=reader.readLine()) != null) {
			
			if (line.contains("<siteinfo>"))
				started = true ;
			
			if (started) {
				duringLineCount++ ;
				sb.append(line + "\n") ;
			} else {
				beforeLineCount++ ;
			}
			
			if (line.contains("</siteinfo>"))
				break ;
			
			if (beforeLineCount > maxBeforeLines)
				break ;
			
			if (duringLineCount > maxDuringLines)
				break ;
			
		}
		reader.close() ;
		
		if (beforeLineCount > maxBeforeLines)
			throw new Exception("Could not detect start of site info element") ;
		
		if (duringLineCount > maxDuringLines)
			throw new Exception("Could not detect end of site info element") ;
		
		Serializer serializer = new Persister();
		return serializer.read(SiteInfo.class, sb.toString()) ;
				
	}
	
	public String getSiteName() {
		return siteName;
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

	public List<Namespace> getNamespaces() {
		return namespaces ;
	}
	
	public Namespace getNamespace(String name) {
		
		return getNamespacesByName().get(name.toLowerCase().trim()) ;
	}
	
	public Namespace getNamespace(int key) {
		
		return getNamespacesByKey().get(key) ;
	}
	
	public Namespace getMainNamespace() {
		return getNamespacesByKey().get(MAIN_KEY) ;
	}

	private Map<String,Namespace> getNamespacesByName() {
		if (namespacesByName != null)
			return namespacesByName ;
		
		namespacesByName = new HashMap<String,Namespace>() ;
		
		for (Namespace namespace:namespaces) 
			namespacesByName.put(namespace.getName().toLowerCase(), namespace) ;
		
			
		return namespacesByName ;
	}
	
	private Map<Integer,Namespace> getNamespacesByKey() {
		if (namespacesByKey != null)
			return namespacesByKey ;
		
		namespacesByKey = new HashMap<Integer,Namespace>() ;
		
		for (Namespace namespace:namespaces) 
			namespacesByKey.put(namespace.getKey(), namespace) ;
		
		return namespacesByKey ;
	}
	
	public static class Namespace {
		
		@Attribute
		private int key ;
		
		@Attribute(name="case") 
		private String caseRule ;
		
		@Text(required=false)
		private String name ;
		
		
		public int getKey() {
			return key;
		}
		
		public String getCaseRule() {
			return caseRule;
		}
		
		public String getName() {
			if (name == null)
				return "" ;
			else
				return name;
		}
		
	}
	
	
	
}
