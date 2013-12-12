package org.wikipedia.miner.web.service;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.wikipedia.miner.annotation.Disambiguator;
import org.wikipedia.miner.annotation.TopicDetector;
import org.wikipedia.miner.annotation.TopicDetector.DisambiguationPolicy;
import org.wikipedia.miner.annotation.preprocessing.DocumentPreprocessor;
import org.wikipedia.miner.annotation.preprocessing.HtmlPreprocessor;
import org.wikipedia.miner.annotation.preprocessing.PreprocessedDocument;
import org.wikipedia.miner.annotation.preprocessing.WikiPreprocessor;
import org.wikipedia.miner.annotation.tagging.DocumentTagger;
import org.wikipedia.miner.annotation.tagging.HtmlTagger;
import org.wikipedia.miner.annotation.tagging.WikiTagger;
import org.wikipedia.miner.annotation.tagging.DocumentTagger.RepeatMode;
import org.wikipedia.miner.annotation.weighting.LinkDetector;
import org.wikipedia.miner.model.Wikipedia;
import org.dmilne.xjsf.Service;
import org.dmilne.xjsf.param.*;
import org.dmilne.xjsf.UtilityMessages.*;

import com.google.gson.annotations.Expose;

@SuppressWarnings("serial")
public class WikifyService extends WMService {
	
	

	public enum SourceMode{AUTO, URL, HTML, WIKI} ;
	public enum LinkFormat{AUTO,WIKI,WIKI_ID,WIKI_ID_WEIGHT,HTML,HTML_ID, HTML_ID_WEIGHT} ;
	
	private StringParameter prmSource ;
	private EnumParameter<SourceMode> prmSourceMode ;
	private EnumParameter<LinkFormat> prmLinkFormat ;
	private FloatParameter prmMinProb ;
	private EnumParameter<RepeatMode> prmRepeatMode ;
	private StringParameter prmLinkStyle ;
	private BooleanParameter prmTooltips ;
	
	private BooleanParameter prmTopics ;
	private BooleanParameter prmReferences ;
	private EnumParameter<DisambiguationPolicy> prmDisambigPolicy ;
	
	private final HashMap<String, TopicDetector> topicDetectors = new HashMap<String, TopicDetector>();
	private final HashMap<String, LinkDetector> linkDetectors = new HashMap<String, LinkDetector>();
	
	private final String linkClassName = "wm_wikifiedLink" ;
	
	private final int maxTokenCount = 10000 ;
	
	public WikifyService() {
		super("core","Augments textual documents with links to the appropriate Wikipedia articles",
				"<p>This service automatically detects the topics mentioned in the given document, and provides links to the appropriate Wikipedia articles. </p>" 
				+ "<p> It doesn't just use Wikipedia as a source of information to link to, but also as training data for how best to do it. In other words, it has been trained to make the same decisions as the people who edit Wikipedia. </p>"
				+ "<p> It may not work very well if the document does not fit the model of what it has been trained on. Documents should not be too short, and should be dedicated to a particular topic.</p>", true
			);
	}
	
	
	@Override
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
	
		
		prmSource = new StringParameter("source", "The document to be wikified (either its content or a web-accessible URL)", null) ;
		addGlobalParameter(prmSource) ;
		
		String[] descSourceMode = {"detect automatically", "web-accessable url", "snippet of html markup", "snippet of mediawiki markup"} ;
		prmSourceMode = new EnumParameter<SourceMode>("sourceMode", "the type of the source document", SourceMode.AUTO, SourceMode.values(), descSourceMode) ;
		addGlobalParameter(prmSourceMode) ;
		
		String[] descLinkFormat = {"WIKI or HTML, depending on source", "as mediawiki markup", "as modified mediawiki markup [[id|anchor]]", "as modified mediawiki markup [[id|weight|anchor]]", "as html links to wikipedia (with '" + linkClassName + "' as the class attribute)", "as modified html links to wikipedia, with pageId as an additional attribute", "as modified html links to wikipedia, with pageId and linkProb as additional attributes"} ;
		prmLinkFormat = new EnumParameter<LinkFormat>("linkFormat", "the format of links", LinkFormat.AUTO, LinkFormat.values(), descLinkFormat) ;
		addGlobalParameter(prmLinkFormat) ;
		
		prmMinProb = new FloatParameter("minProbability", "The system calculates a probability for each topic of whether a Wikipedian would consider it interesting enough to link to. This parameter specifies the minimum probability a topic must have before it will be linked.", (float)0.5) ;
		addGlobalParameter(prmMinProb) ;
		
		String[] descRepeatMode = {"all mentions", "the first mention of each topic", "the first mention of each topic within each region"} ;
		prmRepeatMode = new EnumParameter<RepeatMode>("repeatMode", "whether repeat mentions of topics should be tagged or ignored", RepeatMode.FIRST_IN_REGION, RepeatMode.values(), descRepeatMode) ;
		addGlobalParameter(prmRepeatMode) ;
		
		prmLinkStyle = new StringParameter("linkStyle", "the css style of links. This is only valid if processing a URL" , "") ;
		addGlobalParameter(prmLinkStyle) ;
		
		prmTooltips = new BooleanParameter("tooltips", "<b>true</b> if javascript for adding tooltips should be included, otherwise <b>false</b>. This is only valid if processing a URL.", false) ;
		addGlobalParameter(prmTooltips) ;
		
		prmTopics = new BooleanParameter("topics", "<b>true</b> if to return a list of topics, otherwise <b>false</b> ", true) ;
		addGlobalParameter(prmTopics) ;
		
		prmReferences = new BooleanParameter("references", "<b>true</b> to return details of where each topic was found within text, otherwise <b>false</b>", false) ;
		addGlobalParameter(prmReferences) ;
		
		String[] descDisambigPolicy = {"only one interpretation allowed each term", "multiple interpretations allowed"} ;
		prmDisambigPolicy = new EnumParameter<DisambiguationPolicy>("disambiguationPolicy", "wheither each term should be disambiguated to a single interpretation, or to multiple ones", DisambiguationPolicy.STRICT, DisambiguationPolicy.values(), descDisambigPolicy) ;
		addGlobalParameter(prmDisambigPolicy) ;
		
		for (String wikiName:getWMHub().getWikipediaNames()) {
			
			Wikipedia w = getWMHub().getWikipedia(wikiName) ;
			
			try {
				Disambiguator d = new Disambiguator(w) ;
				d.loadClassifier(w.getConfig().getTopicDisambiguationModel()) ;
				
				TopicDetector td = new TopicDetector(w, d) ;
							
				LinkDetector ld = new LinkDetector(w) ;
				ld.loadClassifier(w.getConfig().getLinkDetectionModel()) ;
				
				topicDetectors.put(wikiName, td) ;
				linkDetectors.put(wikiName, ld) ;
			} catch (Exception e) {
				throw new ServletException(e) ;
			} 
		}
		
		addExample(
				new ExampleBuilder("Wikify a small snippet of text, and view details of the detected topics").
				addParam(prmSource, "At around the size of a domestic chicken, kiwi are by far the smallest living ratites and lay the largest egg in relation to their body size of any species of bird in the world.").
				build()
		) ;
		
		addExample(
				new ExampleBuilder("Wikify a small snippet of text, and view result as html without additional details").
				addParam(prmSource, "At around the size of a domestic chicken, kiwi are by far the smallest living ratites and lay the largest egg in relation to their body size of any species of bird in the world.").
				addParam(prmResponseFormat, ResponseFormat.DIRECT).
				addParam(prmSourceMode, SourceMode.HTML).
				build()
		) ;
		
		addExample(
				new ExampleBuilder("Wikify a web page, and view result as html with added tooltips").
				addParam(prmSource, "http://www.kcc.org.nz/kiwi").
				addParam(prmResponseFormat, ResponseFormat.DIRECT).
				addParam(prmTooltips, true).
				build()
		) ;
		
	}
	
	
	public Service.Message buildWrappedResponse(HttpServletRequest request) throws Exception {
		
		Wikipedia wikipedia = getWikipedia(request) ;
		
		String source = prmSource.getValue(request) ;
		if (source == null || source.trim().length() == 0)
			return new ParameterMissingMessage(request) ;
		
		SourceMode sourceMode = prmSourceMode.getValue(request) ;
		if (sourceMode == SourceMode.AUTO)
			sourceMode = resolveSourceMode(source) ;
		
		
		
		ArrayList<org.wikipedia.miner.annotation.Topic> detectedTopics = new ArrayList<org.wikipedia.miner.annotation.Topic>() ;
		
		String wikifiedDoc = null ;
		try {
			wikifiedDoc = wikifyAndGatherTopics(request, detectedTopics, wikipedia) ;
		} catch (TooLongException e) {
			return new TooLongMessage(request, e) ;
		}
			
			
		double docScore = 0 ;
		for (org.wikipedia.miner.annotation.Topic t:detectedTopics)
			docScore = docScore + t.getRelatednessToOtherTopics() ;
		
		Message msg = new Message(request, wikifiedDoc, sourceMode, docScore) ;
		
		float minProb = prmMinProb.getValue(request) ;
		boolean showTopics = prmTopics.getValue(request) ;
		boolean showReferences = prmReferences.getValue(request) ;
		
		if (showTopics) {
			for (org.wikipedia.miner.annotation.Topic dt:detectedTopics) {
				
				if (dt.getWeight() < minProb) break ;
				
				msg.addTopic(dt, showReferences) ;
			}
		}
		
		return msg;
	}
	
        @Override
	public void buildUnwrappedResponse(HttpServletRequest request, HttpServletResponse response) throws Exception{
		
		Wikipedia wikipedia = getWikipedia(request) ;	
		
		response.setContentType("text/html");
		response.setHeader("Cache-Control", "no-cache"); 
		response.setCharacterEncoding("UTF8") ;
		
		ArrayList<org.wikipedia.miner.annotation.Topic> detectedTopics = new ArrayList<org.wikipedia.miner.annotation.Topic>() ;
		String wikifiedDoc = wikifyAndGatherTopics(request, detectedTopics, wikipedia) ;
		
		response.getWriter().append(wikifiedDoc) ;
	}

	
	private String wikifyAndGatherTopics(HttpServletRequest request, ArrayList<org.wikipedia.miner.annotation.Topic> detectedTopics, Wikipedia wikipedia) throws TooLongException, IOException, Exception {
		
		String wikiName = getWikipediaName(request) ;
		
		TopicDetector topicDetector = topicDetectors.get(wikiName) ;
		LinkDetector linkDetector = linkDetectors.get(wikiName) ;
		
		topicDetector.setDisambiguationPolicy(prmDisambigPolicy.getValue(request)) ;
		
		String source = prmSource.getValue(request) ;
		
		if (source == null || source.trim().equals(""))
			return "" ;
		
		
		SourceMode sourceMode = prmSourceMode.getValue(request) ;
		if (sourceMode == SourceMode.AUTO)
			sourceMode = resolveSourceMode(source) ;
		
		LinkFormat linkFormat = prmLinkFormat.getValue(request) ;
		if (linkFormat == LinkFormat.AUTO) {
			if (sourceMode == SourceMode.WIKI)
				linkFormat = LinkFormat.WIKI ;
			else
				linkFormat = LinkFormat.HTML_ID_WEIGHT ;
		}
		
		String linkStyle = prmLinkStyle.getValue(request) ;
		
		//Vector<Article> bannedTopicList = resolveTopicList(bannedTopics) ;
		
		DocumentPreprocessor dp ;
		if (sourceMode == SourceMode.WIKI) 
			dp = new WikiPreprocessor(wikipedia) ;
		else
			dp = new HtmlPreprocessor() ;
		
		DocumentTagger dt ;
		if (linkFormat == LinkFormat.HTML || linkFormat == LinkFormat.HTML_ID || linkFormat == LinkFormat.HTML_ID_WEIGHT)
			dt = new MyHtmlTagger(linkFormat, linkStyle, wikipedia) ; 
		else
			dt = new MyWikiTagger(linkFormat) ;
		
		
		String markup ;
		if (sourceMode == SourceMode.URL) {
			
			if (source.matches("(?i)^www\\.(.*)$"))
				source = "http://" + source ;
			
			URL url = new URL(source) ;
			
			markup = getWMHub().getRetriever().getWebContent(url) ;
		} else {
			markup = source ;
		}
		
		PreprocessedDocument doc = dp.preprocess(markup) ;
		
		String[] tokens = wikipedia.getConfig().getTokenizer().tokenize(doc.getPreprocessedText()) ;
		if (tokens.length > maxTokenCount) 
			throw new TooLongException(tokens.length, maxTokenCount) ;
		
		
		ArrayList<org.wikipedia.miner.annotation.Topic> allTopics = linkDetector.getWeightedTopics(topicDetector.getTopics(doc, null)) ;
		ArrayList<org.wikipedia.miner.annotation.Topic> bestTopics = new ArrayList<org.wikipedia.miner.annotation.Topic>() ;
		float minProb = prmMinProb.getValue(request) ;
		
		for (org.wikipedia.miner.annotation.Topic t:allTopics) {
			if (t.getWeight() >= minProb)
				bestTopics.add(t) ;
			
			detectedTopics.add(t) ;
		}
		
		String taggedText = dt.tag(doc, bestTopics, prmRepeatMode.getValue(request)) ;
		
		if (sourceMode == SourceMode.URL) {
			taggedText = taggedText.replaceAll("(?i)<html", "<base href=\"" + source  + "\" target=\"_top\"/><html") ;
			
			
			
			
			
			
			
			if (prmTooltips.getValue(request)) {
				
				String basePath = getBasePath(request) ;
				
				if (!basePath.endsWith("/"))
					basePath = basePath + "/" ;
				
				StringBuilder newHeaderStuff = new StringBuilder();
				newHeaderStuff.append("<link type=\"text/css\" rel=\"stylesheet\" href=\"").append(basePath).append("/css/tooltips.css\"/>\n") ;
				newHeaderStuff.append("<link type=\"text/css\" rel=\"stylesheet\" href=\"").append(basePath).append("/css/jquery-ui/jquery-ui-1.8.14.custom.css\"/>\n") ;
				
				String style = prmLinkStyle.getValue(request) ;
				if (style != null && style.trim().length() > 0) 
					newHeaderStuff.append("<style type='text/css'> ." + linkClassName + "{").append(style).append(";}</style>\n") ;
				
				newHeaderStuff.append("<style type='text/css'> .qtip-content div, .qtip-content div p, .qtip-content div b {color:inherit;} </style>") ;
				
				
				newHeaderStuff.append("<script type=\"text/javascript\" src=\"").append(basePath).append("/js/jquery-1.5.1.min.js\"></script>\n") ;
				newHeaderStuff.append("<script type=\"text/javascript\" src=\"").append(basePath).append("/js/jquery.qtip.min.js\"></script>\n") ;
				newHeaderStuff.append("<script type=\"text/javascript\" src=\"").append(basePath).append("/js/tooltips.js\"></script>\n") ;
				newHeaderStuff.append("<script type=\"text/javascript\"> \n") ;
				newHeaderStuff.append("  var wm_host=\"").append(basePath).append("\" ; \n") ;
				newHeaderStuff.append("  $(document).ready(function() { \n") ;
				newHeaderStuff.append("    wm_addDefinitionTooltipsToAllLinks(null, \"" + linkClassName + "\") ; \n") ;
				newHeaderStuff.append("  });\n") ;
				newHeaderStuff.append("</script>\n") ;
				
				taggedText = taggedText.replaceAll("(?i)\\</head>", Matcher.quoteReplacement(newHeaderStuff.toString()) + "</head>") ;		
			}
				
		}		
		
		return taggedText ;
	}
	
	
	
	private SourceMode resolveSourceMode(String source) {
		
		//try to parse source as url
		try {
			//fix omitted http prefix
			if (source.matches("(?i)^www\\.(.*)$"))
				source = "http://" + source ;
			
			@SuppressWarnings("unused")
			URL url = new URL(source) ;
			return SourceMode.URL ;
		} catch (MalformedURLException e) {	} ;
		
		
		//count html elements and wiki link elements
		int htmlCount = 0 ;
		Pattern htmlTag = Pattern.compile("<(.*?)>") ;
		Matcher m = htmlTag.matcher(source) ;
		while (m.find())
			htmlCount++ ;
		
		int wikiCount = 0 ;
		Pattern wikiTag = Pattern.compile("\\[\\[(.*?)\\]\\]") ;
		m = wikiTag.matcher(source) ;
		while (m.find())
			wikiCount++ ;
		
		if (htmlCount > wikiCount)
			return SourceMode.HTML ;
		else
			return SourceMode.WIKI ;
		
	}
	
	
	
	
	
	
	private class MyHtmlTagger extends HtmlTagger{

		LinkFormat linkFormat ;
		String linkStyle ;
		Wikipedia wikipedia ;

		protected MyHtmlTagger(LinkFormat linkFormat, String linkStyle, Wikipedia wikipedia) {		
			this.linkFormat = linkFormat ;
			this.linkStyle = linkStyle ;
			if (this.linkStyle != null)
				this.linkStyle = this.linkStyle.trim();
			
			this.wikipedia = wikipedia ;
		}
				
                @Override
		public String getTag(String anchor, org.wikipedia.miner.annotation.Topic topic) {
			
			StringBuilder tag = new StringBuilder("<a") ;
			tag.append(" href=\"http://www.").append(wikipedia.getConfig().getLangCode()).append(".wikipedia.org/wiki/").append(topic.getTitle()).append("\"") ;
			
			tag.append(" class=\"" + linkClassName + "\"") ;
			
			if (linkFormat == LinkFormat.HTML_ID || linkFormat == LinkFormat.HTML_ID_WEIGHT) 
				tag.append(" pageId=\"").append(topic.getId()).append("\"") ;
			
			if (linkFormat == LinkFormat.HTML_ID_WEIGHT) 
				tag.append(" linkProb=\"").append(getHub().format(topic.getWeight())).append("\"") ; 
			
			if (linkStyle != null && linkStyle.length() > 0) 
				tag.append(" style=\"").append(linkStyle).append("\"") ;
			
			tag.append(">") ;
			tag.append(anchor) ;
			tag.append("</a>") ;
			
			return tag.toString() ;
		}	
	}
	
	
	
	private class MyWikiTagger extends WikiTagger {
		
		LinkFormat linkFormat ;
				
		MyWikiTagger(LinkFormat linkFormat) {		
			this.linkFormat = linkFormat ;
		}
		
                @Override
		public String getTag(String anchor, org.wikipedia.miner.annotation.Topic topic) {
			
			StringBuilder tag = new StringBuilder("[[") ;
			
			if (linkFormat == LinkFormat.WIKI_ID || linkFormat == LinkFormat.WIKI_ID_WEIGHT) {
				tag.append(topic.getId()) ;
				
				if (linkFormat == LinkFormat.WIKI_ID_WEIGHT) {
					tag.append("|") ;
					tag.append(getHub().format(topic.getWeight())) ;
				}
				
				tag.append("|") ;
				tag.append(anchor) ;
				
			} else {
				
				if (topic.getTitle().compareToIgnoreCase(anchor) == 0)
					tag.append(anchor) ;
				else {
					tag.append(topic.getTitle()) ;
					tag.append("|") ;
					tag.append(anchor) ;
				}
			}
			
			tag.append("]]") ;
			return tag.toString() ;
		}
	}
	
	public static class Message extends Service.Message {
		
		@Expose
		@Element(data=true)	
		private final String wikifiedDocument ;

		@Expose
		@Attribute
		private final SourceMode sourceMode ;
		
		@Expose
		@Attribute
		private final double documentScore ;
		
		@Expose
		@ElementList(entry="detectedTopic")
		private ArrayList<Topic> detectedTopics ;
		
		private Message(HttpServletRequest request, String wikifiedDoc, SourceMode sourceMode, double documentScore) {
			
			super(request) ;
			
			this.wikifiedDocument = wikifiedDoc ;
			this.sourceMode = sourceMode ;
			this.documentScore = documentScore ;
			
			this.detectedTopics = new ArrayList<Topic>() ;
		}
		
		private void addTopic(org.wikipedia.miner.annotation.Topic t, boolean includePositions) {
			
			if (this.detectedTopics == null) 
				this.detectedTopics = new ArrayList<Topic>() ;
			
			this.detectedTopics.add(new Topic(t, includePositions)) ;
		}

		public String getWikifiedDocument() {
			return wikifiedDocument;
		}

		public SourceMode getSourceMode() {
			return sourceMode;
		}

		public double getDocumentScore() {
			return documentScore;
		}

		public List<Topic> getDetectedTopics() {
			return Collections.unmodifiableList(detectedTopics);
		}
	}
	
	public static class Topic {
		
		@Expose
		@Attribute
		private final int id ;
		
		@Expose
		@Attribute
		private final String title ;
		
		@Expose
		@Attribute
		private final double weight ;
		
		@Expose
		@ElementList(entry="reference", required=false) 
		private ArrayList<Reference> references ;
		
		private Topic(org.wikipedia.miner.annotation.Topic t, boolean includeReferences) {
			
			this.id = t.getId();
			this.title = t.getTitle() ;
			this.weight = t.getWeight();

			
			if (includeReferences) {
				references = new ArrayList<Reference>() ;
				
				for (org.wikipedia.miner.util.Position p: t.getPositions()) {
					references.add(new Reference(p.getStart(), p.getEnd())) ;
				}
			}
		}

		public int getId() {
			return id;
		}

		public String getTitle() {
			return title;
		}

		public double getWeight() {
			return weight;
		}

		public List<Reference> getReferences() {
			
			if (references == null) return Collections.unmodifiableList(new ArrayList<Reference>()) ;
			
			return Collections.unmodifiableList(references);
		}
		
	}
	
	public static class Reference {
		
		@Expose
		@Attribute
		private final int start ;
		
		@Expose
		@Attribute
		private final int end ;
		
		private Reference(int start, int end) {
			this.start = start ;
			this.end = end ;
		}

		public int getStart() {
			return start;
		}

		public int getEnd() {
			return end;
		}
	}
	
	public static class TooLongMessage extends ErrorMessage {

		@Expose
		@Attribute
		private final int tokenCount ;
		
		@Expose
		@Attribute
		private final int maxTokenCount ;
	
		protected TooLongMessage(HttpServletRequest httpRequest, TooLongException e) {
			super(httpRequest, e);
			
			this.tokenCount = e.getTokenCount() ;
			this.maxTokenCount = e.getMaxTokenCount() ;
		}

		public int getTokenCount() {
			return tokenCount;
		}

		public int getMaxTokenCount() {
			return maxTokenCount;
		}
	}
	
	
	public static class TooLongException extends Exception {
		
		private final int tokenCount ;
		private final int maxTokenCount ;
		
		public TooLongException(int tokenCount, int maxTokenCount) {
			
			super("Input document is too long") ;
			
			this.tokenCount = tokenCount ;
			this.maxTokenCount = maxTokenCount ;
		}

		public int getTokenCount() {
			return tokenCount;
		}

		public int getMaxTokenCount() {
			return maxTokenCount;
		}
	}
	
}
