package org.wikipedia.miner.extract.util;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Root;
import org.simpleframework.xml.Serializer;
import org.simpleframework.xml.Transient;
import org.simpleframework.xml.core.Persister;

@Root
public class Languages {

	@ElementList(inline=true, entry="Language")
	private List<Language> languages;

	@Transient
	private Map<String,Integer> languageIndexesByCode ;

	private Map<String,Integer> getLanguageIndexesByCode() {
		if (languageIndexesByCode != null)
			return languageIndexesByCode ;
		
		languageIndexesByCode = new HashMap<String,Integer>() ;
		
		int index = 0 ;
		for (Language lang:languages) {
			languageIndexesByCode.put(lang.getCode(), index) ;
			index++ ;
		}
			
		return languageIndexesByCode ;
		
	}
	
	public Language get(String code) {
		Integer index = getLanguageIndexesByCode().get(code) ;
		
		if (index == null)
			return null ;
		
		return languages.get(index) ;
	}
	
	public static Languages load(File file) throws Exception {
		
		Serializer serializer = new Persister();
		return serializer.read(Languages.class, file);
		
	}
	
	public static Languages load(InputStream input) throws Exception {
		
		Serializer serializer = new Persister();
		return serializer.read(Languages.class, input) ;
		
	}
	

	public static class Language {

		@Attribute
		private String code ;

		@Attribute 
		private String name ;

		@Attribute
		private String localName ;


		@Element(name="RootCategory")
		private String rootCategory ;

		@ElementList(inline=true, entry="DisambiguationCategory")
		private List<String> disambigCategories ;

		@ElementList(inline=true, entry="DisambiguationTemplate")
		private List<String> disambigTemplates ;

		@ElementList(inline=true, entry="RedirectIdentifier")
		private List<String> redirectIdentifiers ;

		@ElementList(inline=true, required=false, entry="NamespaceAlias")
		private List<NamespaceAlias> namespaceAliases ;
		
		

		@Transient
		private Pattern disambigPattern ;


		@Transient
		private Pattern redirectPattern ;
		
		@Transient
		private Map<String,Integer> aliasMap ;


		public String getCode() {
			return code;
		}

		public String getName() {
			return name;
		}

		public String getLocalName() {
			return localName;
		}

		public String getRootCategory() {
			return rootCategory;
		}

		public List<String> getDisambigCategories() {
			return disambigCategories;
		}

		public List<String> getDisambigTemplates() {
			return disambigTemplates;
		}

		public List<String> getRedirectIdentifiers() {
			return redirectIdentifiers;
		}

		public List<NamespaceAlias> getNamespaceAliases() {
			return namespaceAliases;
		}

		public Pattern getDisambigPattern() {

			if (disambigPattern != null)
				return disambigPattern ;

			String disambigCategoryRegex = null ;
			if (!disambigCategories.isEmpty()) {

				StringBuffer tmp = new StringBuffer() ;

				tmp.append("\\[\\[\\s*") ;

				if (disambigCategories.size() == 1) {
					tmp.append(disambigCategories.get(0)) ;
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
					tmp.append(disambigTemplates.get(0)) ;
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
				throw new NullPointerException("language configuration does not specify any categories or templates for identifying disambiguation pages") ;
			}

			if (disambigCategoryRegex != null && disambigTemplateRegex != null) {
				disambigPattern = Pattern.compile("(" + disambigCategoryRegex + "|" + disambigTemplateRegex + ")", Pattern.CASE_INSENSITIVE) ;
			} else {
				if (disambigCategoryRegex != null)
					disambigPattern = Pattern.compile(disambigCategoryRegex, Pattern.CASE_INSENSITIVE) ;
				else
					disambigPattern = Pattern.compile(disambigTemplateRegex, Pattern.CASE_INSENSITIVE) ;
			}

			return disambigPattern ;
		}

		public Pattern getRedirectPattern() {

			if (redirectPattern != null)
				return redirectPattern ;

			StringBuffer redirectRegex = new StringBuffer("\\#") ;
			redirectRegex.append("(") ;
			for (String ri:redirectIdentifiers) {
				redirectRegex.append(ri) ;
				redirectRegex.append("|") ;
			}
			redirectRegex.deleteCharAt(redirectRegex.length()-1) ;
			redirectRegex.append(")[:\\s]*(?:\\[\\[(.*)\\]\\]|(.*))") ;

			redirectPattern = Pattern.compile(redirectRegex.toString(), Pattern.CASE_INSENSITIVE) ;

			return redirectPattern ;
		}
		
		
		private Map<String,Integer> getAliasMap() {
			if (aliasMap != null)
				return aliasMap ;
			
			aliasMap = new HashMap<String,Integer>() ;
			int index = 0 ;
			for (NamespaceAlias alias:namespaceAliases) {
				aliasMap.put(alias.from.toLowerCase(), index) ;
				index++ ;
			}
			
			return aliasMap ;
		}
		
		public NamespaceAlias getAlias(String fromNamespace) {
			
			Integer index = getAliasMap().get(fromNamespace.toLowerCase()) ;
			if (index == null)
				return null ;
			
			return namespaceAliases.get(index) ;
		}
	}


	public static class NamespaceAlias {

		public String getFrom() {
			return from;
		}

		public String getTo() {
			return to;
		}

		@Attribute
		private String from ;

		@Attribute
		private String to ;


	}

}

