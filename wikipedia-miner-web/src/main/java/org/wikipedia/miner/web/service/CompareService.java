package org.wikipedia.miner.web.service;

import gnu.trove.set.hash.TLongHashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Text;
import org.wikipedia.miner.comparison.ArticleComparer;
import org.wikipedia.miner.comparison.ConnectionSnippet;
import org.wikipedia.miner.comparison.ConnectionSnippetWeighter;
import org.wikipedia.miner.comparison.LabelComparer;
import org.wikipedia.miner.model.Article;
import org.wikipedia.miner.model.Label;
import org.wikipedia.miner.model.Page.PageType;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.RelatednessCache;
import org.wikipedia.miner.util.text.TextProcessor;
import org.wikipedia.miner.web.util.UtilityMessages.InvalidIdMessage;
import org.wikipedia.miner.web.util.UtilityMessages.UnknownTermMessage;
import org.dmilne.xjsf.Service;
import org.dmilne.xjsf.UtilityMessages.ErrorMessage;
import org.dmilne.xjsf.UtilityMessages.ParameterMissingMessage;
import org.dmilne.xjsf.param.BooleanParameter;
import org.dmilne.xjsf.param.FloatParameter;
import org.dmilne.xjsf.param.IntListParameter;
import org.dmilne.xjsf.param.IntParameter;
import org.dmilne.xjsf.param.ParameterGroup;
import org.dmilne.xjsf.param.StringParameter;

import com.google.gson.annotations.Expose;
import org.dmilne.xjsf.UtilityMessages;
import org.wikipedia.miner.util.NGrammer;
import org.wikipedia.miner.web.util.xjsfParameters.StringListParameter;

/**
 *
 * This web service measures relatedness between:
 * <ul>
 * <li>pairs of terms (or phrases)</li>
 * <li>pairs of article ids</li>
 * <li>lists of article ids</li>
 * </ul>
 *
 * Pairs of terms will be automatically disambiguated against each other, to be
 * resolved to pairs of articles. If comparing pairs of terms or pairs of
 * article ids, then users can also obtain connective topics (articles that link
 * to both things being compared) and explanatory sentences (which mention both
 * things being compared).
 *
 * @author David Milne
 */
public class CompareService extends WMService {

    private static final long serialVersionUID = 537957588352023198L;

    private ParameterGroup grpTerms;
    private StringParameter prmTerm1;
    private StringParameter prmTerm2;

    private ParameterGroup grpListTerms;
    private StringListParameter prmListTerm1;
    private StringListParameter prmListTerm2;

    private ParameterGroup grpIds;
    private IntParameter prmId1;
    private IntParameter prmId2;

    private ParameterGroup grpIdLists;
    private IntListParameter prmIdList1;
    private IntListParameter prmIdList2;

    private BooleanParameter prmDisambiguation;
    private BooleanParameter prmConnections;
    private BooleanParameter prmSnippets;
    private BooleanParameter prmTitles;

    private IntParameter prmMaxConsConsidered;
    private IntParameter prmMaxConsReturned;
    private IntParameter prmMaxSnippetsConsidered;
    private IntParameter prmMaxSnippetsReturned;

    //private BooleanParameter prmEscape ;
    private FloatParameter prmMinRelatedness;

    private enum GroupName {

        termPair, termList, idPair, idLists, none
    };

    /**
     * Initialises a new CompareService
     */
    public CompareService() {

        super("core", "Measures and explains the connections between Wikipedia articles ",
                "<p>This service measures the semantic relatedness between pairs of terms, pairs of article ids, or sets of article ids.</p>"
                + "<p>The relatedness measures are calculated from the links going into and out of each page. Links that are common to both pages are used as evidence that they are related, while links that are unique to one or the other indicate the opposite.</p>",
                true
        );
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        grpTerms = new ParameterGroup(GroupName.termPair.name(), "To compare two (potentially ambiguous) terms");
        prmTerm1 = new StringParameter("term1", "The first of two terms (or phrases) to compare", null);
        grpTerms.addParameter(prmTerm1);
        prmTerm2 = new StringParameter("term2", "The second of two terms (or phrases) to compare", null);
        grpTerms.addParameter(prmTerm2);
        prmDisambiguation = new BooleanParameter("disambiguationDetails", "if <b>true</b>, then the service will list different interpretations (combinations of senses for ambiguous terms) that were considered.", false);
        grpTerms.addParameter(prmDisambiguation);
        addParameterGroup(grpTerms);

        grpListTerms = new ParameterGroup((GroupName.termList.name()), "To compare two lists of (potentially ambiguous)terms, will disambiguate them if needed");
        prmListTerm1 = new StringListParameter("term1List", "The first list of terms (or phrases) to compare", null);
        prmListTerm2 = new StringListParameter("term2List", "The second list of terms (or phrases) to compare", null);
        grpListTerms.addParameter(prmListTerm1);
        grpListTerms.addParameter(prmListTerm2);
        addParameterGroup(grpListTerms);

        grpIds = new ParameterGroup(GroupName.idPair.name(), "To compare two (unambiguous) article ids");
        prmId1 = new IntParameter("id1", "The first of two article ids to compare", null);
        grpIds.addParameter(prmId1);
        prmId2 = new IntParameter("id2", "The second of two article ids to compare", null);
        grpIds.addParameter(prmId2);
        addParameterGroup(grpIds);

        grpIdLists = new ParameterGroup(GroupName.idLists.name(), "To compare multiple article ids");
        prmIdList1 = new IntListParameter("ids1", "A list of page ids to compare", null);
        grpIdLists.addParameter(prmIdList1);
        prmIdList2 = new IntListParameter("ids2", "A second list of page ids to compare. If this is specified, then each article in <em>ids1</em> will be compared against every article in <em>ids2</em>. Otherwise, every article in <em>ids1</em> will be compared against every other article in <em>ids1</em>", new Integer[0]);
        grpIdLists.addParameter(prmIdList2);
        prmMinRelatedness = new FloatParameter("minRelatedness", "The minimum relatedness a term pair must have before it will be returned.", 0F);
        grpIdLists.addParameter(prmMinRelatedness);
        addParameterGroup(grpIdLists);

        prmConnections = new BooleanParameter("connections", "if <b>true</b>, then the service will list articles that refer to both topics being compared. This parameter is ignored if comparing lists of ids.", false);
        addGlobalParameter(prmConnections);

        prmMaxConsConsidered = new IntParameter("maxConnectionsConsidered", "The maximum number of connections that will be gathered and weighted by thier relatedness to the articles being compared. This parameter is ignored if comparing lists of ids.", 1000);
        addGlobalParameter(prmMaxConsConsidered);

        prmMaxConsReturned = new IntParameter("maxConnectionsReturned", "The maximum number of connections that will be returned. These will be the highest weighted connections. This parameter is ignored if comparing lists of ids.", 250);
        addGlobalParameter(prmMaxConsReturned);

        prmSnippets = new BooleanParameter("snippets", "if <b>true</b>, then the service will list sentences that either mention both of the articles being compared, or come from one of the articles and mention the other. This parameter is ignored if comparing lists of ids.", false);
        addGlobalParameter(prmSnippets);

        prmMaxSnippetsConsidered = new IntParameter("maxSnippetsConsidered", "The maximum number of snippets that will be gathered and weighted. This parameter is ignored if comparing lists of ids.", 30);
        addGlobalParameter(prmMaxSnippetsConsidered);

        prmMaxSnippetsReturned = new IntParameter("maxSnippets", "The maximum number of snippets that will be returned. These will be the highest weighted snippets. This parameter is ignored if comparing lists of ids.", 10);
        addGlobalParameter(prmMaxSnippetsReturned);

        prmTitles = new BooleanParameter("titles", "if <b>true</b>, then the corresponding titles for article ids will be returned. This parameter is ignored if comparing terms", false);
        addGlobalParameter(prmTitles);

        addGlobalParameter(getWMHub().getFormatter().getEmphasisFormatParam());
        addGlobalParameter(getWMHub().getFormatter().getLinkFormatParam());

        addExample(
                new ExampleBuilder("To measure the relatedness between <i>kiwi</i> and <i>takahe</i>").
                addParam(prmTerm1, "kiwi").
                addParam(prmTerm2, "takahe").
                build()
        );

        addExample(
                new ExampleBuilder("To see full details of the same comparison").
                addParam(prmTerm1, "kiwi").
                addParam(prmTerm2, "takahe").
                addParam(prmDisambiguation, true).
                addParam(prmConnections, true).
                addParam(prmSnippets, true).
                build()
        );

        Integer[] kiwi = {17362};
        Integer[] otherBirds = {711147, 89074, 89073};

        addExample(
                new ExampleBuilder("To compare <i>kiwi</i> to <i>takahe</i>, <i>kakapo</i> and <i>kea</i>").
                addParam(prmIdList1, kiwi).
                addParam(prmIdList2, otherBirds).
                addParam(prmTitles, true).
                build()
        );

        //prmEscape = new BooleanParameter("escapeDefinition", "<true> if sentence snippets should be escaped, <em>false</em> if they are to be encoded directly", false) ;
        //addGlobalParameter(prmEscape) ;
    }

    @Override
    public Service.Message buildWrappedResponse(HttpServletRequest request) throws Exception {

        Wikipedia wikipedia = getWikipedia(request);
        TextProcessor tp = wikipedia.getEnvironment().getConfiguration().getDefaultTextProcessor();

        ParameterGroup grp = getSpecifiedParameterGroup(request);

        if (grp == null) {
            return new ParameterMissingMessage(request);
        }

        Message msg = null;

        switch (GroupName.valueOf(grp.getName())) {

            case termPair:

                LabelComparer lblComparer = getWMHub().getLabelComparer(getWikipediaName(request));
                if (lblComparer == null) {
                    return new ErrorMessage(request, "term comparisons are not available with this wikipedia instance");
                }

                String term1 = prmTerm1.getValue(request).trim();
                String term2 = prmTerm2.getValue(request).trim();

                Label label1 = new Label(wikipedia.getEnvironment(), term1, tp);
                Label.Sense[] senses1 = label1.getSenses();

                if (senses1.length == 0) {
                    return new UnknownTermMessage(request, term1);
                }

                Label label2 = new Label(wikipedia.getEnvironment(), term2, tp);
                Label.Sense[] senses2 = label2.getSenses();

                if (senses2.length == 0) {
                    return new UnknownTermMessage(request, term2);
                }

                LabelComparer.ComparisonDetails details = lblComparer.compare(label1, label2);

                msg = new Message(request, details.getLabelRelatedness());

                if (!prmDisambiguation.getValue(request)) {
                    msg.addDisambiguationDetails(details);
                }

                LabelComparer.SensePair bestInterpretation = details.getBestInterpretation();
                if (bestInterpretation != null) {
                    msg = addMutualLinksOrSnippets(msg, bestInterpretation.getSenseA(), bestInterpretation.getSenseB(), request, wikipedia);
                }
                //else 
                //	this.buildWarningResponse("Could not identify plausable senses for the given terms", xmlResponse) ;

                return msg;
            case termList:
                ArticleComparer artiComparer = getWMHub().getArticleComparer(getWikipediaName(request));
                if (artiComparer == null) {
                    return new ErrorMessage(request, "article comparisons are not available with this wikipedia instance");
                }
                LabelComparer lbComparer = getWMHub().getLabelComparer(getWikipediaName(request));
                if (lbComparer == null) {
                    return new ErrorMessage(request, "label comparisons are not available with this wikipedia instance");
                }
                if (prmListTerm1.getValue(request) == null || prmListTerm2.getValue(request) == null) {
                    return new ParameterMissingMessage(request);
                }
                Message mess = new Message(request);
                String[] term2List = prmListTerm2.getValue(request);
                List<org.wikipedia.miner.model.Label> labels = new ArrayList<org.wikipedia.miner.model.Label>();
                NGrammer nGrammer = new NGrammer(wikipedia.getConfig().getSentenceDetector(), wikipedia.getConfig().getTokenizer());
                for (String string2 : term2List) {
                    NGrammer.NGramSpan span2 = nGrammer.ngramPosDetect(string2)[0];
                    org.wikipedia.miner.model.Label lab2 = wikipedia.getLabel(span2, string2);
                    labels.add(lab2);
                }
                String[] term1List = prmListTerm1.getValue(request);
                List<String> invalidTerm = new ArrayList<String>();
                //TODO use TreeMap? at least for me its better to have the same order as the input
                
                for (String string1 : term1List) {
                    NGrammer.NGramSpan span = nGrammer.ngramPosDetect(string1)[0];
                    org.wikipedia.miner.model.Label lab1 = wikipedia.getLabel(span, string1);
                    Label.Sense[] sen1 = lab1.getSenses();
                    if (sen1.length != 0) {
                        int j=0;
                        for (Label lab2 : labels) {
                            Label.Sense[] sen2 = lab2.getSenses();
                            if (sen2.length != 0) {
                                LabelComparer.ComparisonDetails dets = lbComparer.compare(lab1, lab2);
                                mess.addDisambiguationDetailsList(dets, string1,term2List[j]);
                            }
                            j++;
                        }
                    }
                }

                return mess;

            case idPair:

                ArticleComparer artComparer = getWMHub().getArticleComparer(getWikipediaName(request));
                if (artComparer == null) {
                    return new ErrorMessage(request, "article comparisons are not available with this wikipedia instance");
                }

                Article art1 = new Article(wikipedia.getEnvironment(), prmId1.getValue(request));
                if (!(art1.getType() == PageType.article || art1.getType() == PageType.disambiguation)) {
                    return new InvalidIdMessage(request, prmId1.getValue(request));
                }

                Article art2 = new Article(wikipedia.getEnvironment(), prmId2.getValue(request));
                if (!(art2.getType() == PageType.article || art2.getType() == PageType.disambiguation)) {
                    return new InvalidIdMessage(request, prmId2.getValue(request));
                }

                msg = new Message(request, artComparer.getRelatedness(art1, art2));

                if (prmTitles.getValue(request)) {
                    msg.setTitles(art1.getTitle(), art2.getTitle());
                }

                msg = addMutualLinksOrSnippets(msg, art1, art2, request, wikipedia);
                break;
            case idLists:

                artComparer = getWMHub().getArticleComparer(getWikipediaName(request));
                if (artComparer == null) {
                    return new ErrorMessage(request, "article comparisons are not available with this wikipedia instance");
                }

                msg = new Message(request);

                //gather articles from ids1 ;
                TreeSet<Article> articles1 = new TreeSet<Article>();
                for (Integer id : prmIdList1.getValue(request)) {
                    try {
                        Article art = (Article) wikipedia.getPageById(id);
                        articles1.add(art);
                    } catch (Exception e) {
                        msg.addInvalidId(id);
                    }
                }

                //gather articles from ids2 ;
                TreeSet<Article> articles2 = new TreeSet<Article>();
                for (Integer id : prmIdList2.getValue(request)) {
                    try {
                        Article art = (Article) wikipedia.getPageById(id);
                        articles2.add(art);
                    } catch (Exception e) {
                        msg.addInvalidId(id);
                    }
                }

                //if ids2 is not specified, then we want to compare each item in ids1 with every other one
                if (articles2.isEmpty()) {
                    articles2 = articles1;
                }

                TLongHashSet doneKeys = new TLongHashSet();

                float minRelatedness = prmMinRelatedness.getValue(request);
                boolean showTitles = prmTitles.getValue(request);

                for (Article a1 : articles1) {
                    for (Article a2 : articles2) {

                        if (a1.equals(a2)) {
                            continue;
                        }

                        //relatedness is symmetric, so create a unique key for this pair of ids were order doesnt matter 
                        Article min, max;

                        if (a1.getId() < a2.getId()) {
                            min = a1;
                            max = a2;
                        } else {
                            min = a2;
                            max = a1;
                        }

                        //long min = Math.min(a1.getId(), a2.getId()) ;
                        //long max = Math.max(a1.getId(), a2.getId()) ;
                        long key = ((long) min.getId()) + (((long) max.getId()) << 30);

                        if (doneKeys.contains(key)) {
                            continue;
                        }

                        double relatedness = artComparer.getRelatedness(a1, a2);

                        if (relatedness >= minRelatedness) {
                            msg.addComparison(new Comparison(min, max, relatedness, showTitles));
                        }

                        doneKeys.add(key);
                    }
                }
                break;
        }

        if (msg == null) {
            return new ErrorMessage(request, "nothing to do");
        } else {
            return msg;
        }
    }

    private Message addMutualLinksOrSnippets(Message msg, Article art1, Article art2, HttpServletRequest request, Wikipedia wikipedia) throws Exception {

        boolean getConnections = prmConnections.getValue(request);
        boolean getSnippets = prmSnippets.getValue(request);

        if (!getConnections && !getSnippets) {
            return msg;
        }

        //Build a list of pages that link to both art1 and art2, ordered by average relatedness to them
        TreeSet<Article> connections = new TreeSet<Article>();
        RelatednessCache rc = new RelatednessCache(getWMHub().getArticleComparer(getWikipediaName(request)));

        Article[] links1 = art1.getLinksIn();
        Article[] links2 = art2.getLinksIn();

        int index1 = 0;
        int index2 = 0;

        int maxConsConsidered = prmMaxConsConsidered.getValue(request);

        while (index1 < links1.length && index2 < links2.length) {

            Article link1 = links1[index1];
            Article link2 = links2[index2];

            int compare = link1.compareTo(link2);

            if (compare == 0) {
                if (link1.compareTo(art1) != 0 && link2.compareTo(art2) != 0) {

                    double weight = (rc.getRelatedness(link1, art1) + rc.getRelatedness(link1, art2)) / 2;
                    link1.setWeight(weight);
                    connections.add(link1);

                    if (connections.size() > maxConsConsidered) {
                        break;
                    }
                }

                index1++;
                index2++;
            } else {
                if (compare < 0) {
                    index1++;
                } else {
                    index2++;
                }
            }
        }

        int maxConsReturned = prmMaxConsReturned.getValue(request);

        if (getConnections) {

            int c = 0;

            for (Article connection : connections) {
                if (c++ >= maxConsReturned) {
                    break;
                }
                msg.addConnection(new Connection(connection, rc.getRelatedness(connection, art1), rc.getRelatedness(connection, art2)));
            }
        }

        if (getSnippets) {

            int maxSnippetsConsidered = prmMaxSnippetsConsidered.getValue(request);

            //gather and weight snippets
            ConnectionSnippetWeighter snippetWeighter = getWMHub().getConnectionSnippetWeighter(getWikipediaName(request));

            TreeSet<ConnectionSnippet> snippets = new TreeSet<ConnectionSnippet>();

            //look for snippets in art1 which mention art2
            for (int sentenceIndex : art1.getSentenceIndexesMentioning(art2)) {
                ConnectionSnippet s = new ConnectionSnippet(sentenceIndex, art1, art1, art2);
                s.setWeight(snippetWeighter.getWeight(s));
                snippets.add(s);
            }

            //look for snippets in art2 which mention art1
            for (int sentenceIndex : art2.getSentenceIndexesMentioning(art1)) {
                ConnectionSnippet s = new ConnectionSnippet(sentenceIndex, art2, art1, art2);
                s.setWeight(snippetWeighter.getWeight(s));
                snippets.add(s);
            }

            ArrayList<Article> articlesOfInterest = new ArrayList<Article>();
            articlesOfInterest.add(art1);
            articlesOfInterest.add(art2);

            for (Article connection : connections) {

                if (snippets.size() >= maxSnippetsConsidered) {
                    break;
                }

                for (int sentenceIndex : connection.getSentenceIndexesMentioning(articlesOfInterest)) {
                    if (snippets.size() >= maxSnippetsConsidered) {
                        break;
                    }

                    ConnectionSnippet s = new ConnectionSnippet(sentenceIndex, connection, art1, art2);
                    s.setWeight(snippetWeighter.getWeight(s));
                    snippets.add(s);
                }
            }

            Pattern labelPattern = getLabelMatchingPattern(art1, art2);

            int maxSnippetsReturned = prmMaxSnippetsReturned.getValue(request);

            int snippetsReturned = 0;
            for (ConnectionSnippet snippet : snippets) {
                snippetsReturned++;
                if (snippetsReturned > maxSnippetsReturned) {
                    break;
                }

                String formattedMarkup = snippet.getPlainText();
                formattedMarkup = emphasizePatternMatches(formattedMarkup, labelPattern);
                formattedMarkup = getWMHub().getFormatter().format(formattedMarkup, request, wikipedia);

                msg.addSnippet(new Snippet(snippet, formattedMarkup));
            }
        }

        return msg;
    }

    private String emphasizePatternMatches(String sentence, Pattern pattern) {

        String sentence2 = " " + sentence + " ";

        Matcher m = pattern.matcher(sentence2);

        StringBuilder sb = new StringBuilder();
        int lastCopyIndex = 0;

        while (m.find()) {
            sb.append(sentence2.substring(lastCopyIndex, m.start()));
            sb.append("'''");
            sb.append(m.group());
            sb.append("'''");
            lastCopyIndex = m.end();
        }
        sb.append(sentence2.substring(lastCopyIndex));

        return sb.toString().trim();
    }

    private Pattern getLabelMatchingPattern(Article art1, Article art2) {
        StringBuilder labelRegex = new StringBuilder("(?<=\\W)(");

        for (Article.Label lbl : art1.getLabels()) {

            if (lbl.getLinkOccCount() > 3) {
                labelRegex.append(lbl.getText());
                labelRegex.append("|");
            }
        }

        for (Article.Label lbl : art2.getLabels()) {

            if (lbl.getLinkOccCount() > 3) {
                labelRegex.append(lbl.getText());
                labelRegex.append("|");
            }
        }

        labelRegex.deleteCharAt(labelRegex.length() - 1);
        labelRegex.append(")(?=\\W)");

        return Pattern.compile(labelRegex.toString(), Pattern.CASE_INSENSITIVE);
    }

    public static class Message extends Service.Message {

        @Expose
        @Attribute(required = false)
        private final Double relatedness;

        @Expose
        @Attribute(required = false)
        private String title1;

        @Expose
        @Attribute(required = false)
        private String title2;

        @Expose
        @Element(required = false)
        private List<DisambiguationDetails> disambiguationDetailsList;

        @Expose
        @Element(required = false)
        private DisambiguationDetails disambiguationDetails;

        @Expose
        @ElementList(required = false, entry = "connection")
        private ArrayList<Connection> connections;

        @Expose
        @ElementList(required = false, entry = "snippet")
        private ArrayList<Snippet> snippets;

        @Expose
        @ElementList(required = false, entry = "comparison")
        private ArrayList<Comparison> comparisons;

        @Expose
        @ElementList(required = false, entry = "invalidId")
        private TreeSet<Integer> invalidIds;

        private Message(HttpServletRequest request) {
            super(request);
            this.relatedness = null;
        }

        private Message(HttpServletRequest request, double relatedness) {
            super(request);
            this.relatedness = relatedness;
        }

        private void setTitles(String title1, String title2) {
            this.title1 = title1;
            this.title2 = title2;
        }

        private void addDisambiguationDetails(LabelComparer.ComparisonDetails details) {
            disambiguationDetails = new DisambiguationDetails(details);
        }

        private void addDisambiguationDetailsList(LabelComparer.ComparisonDetails details) {
            if (disambiguationDetailsList == null) {
                disambiguationDetailsList = new ArrayList<DisambiguationDetails>();
            }
            disambiguationDetailsList.add(new DisambiguationDetails(details));
        }

        private void addDisambiguationDetailsList(LabelComparer.ComparisonDetails details, String term1, String term2) {
            if (disambiguationDetailsList == null) {
                disambiguationDetailsList = new ArrayList<DisambiguationDetails>();
            }
            disambiguationDetailsList.add(new DisambiguationDetails(details, term1, term2));
        }

        private void addConnection(Connection c) {
            if (connections == null) {
                connections = new ArrayList<Connection>();
            }
            connections.add(c);
        }

        private void addSnippet(Snippet s) {
            if (snippets == null) {
                snippets = new ArrayList<Snippet>();
            }

            snippets.add(s);
        }

        private void addComparison(Comparison c) {
            if (comparisons == null) {
                comparisons = new ArrayList<Comparison>();
            }

            comparisons.add(c);
        }

        private void addInvalidId(Integer id) {
            if (invalidIds == null) {
                invalidIds = new TreeSet<Integer>();
            }

            invalidIds.add(id);
        }

        public Double getRelatedness() {
            return relatedness;
        }

        public String getTitle1() {
            return title1;
        }

        public String getTitle2() {
            return title2;
        }

        public DisambiguationDetails getDisambiguationDetails() {
            return disambiguationDetails;
        }

        public List<DisambiguationDetails> getDisambiguationDetailsList() {
            return Collections.unmodifiableList(disambiguationDetailsList);
        }

        public List<Connection> getConnections() {
            return Collections.unmodifiableList(connections);
        }

        public List<Snippet> getSnippets() {
            return Collections.unmodifiableList(snippets);
        }

        public List<Comparison> getComparisons() {
            return Collections.unmodifiableList(comparisons);
        }

        public SortedSet<Integer> getInvalidIds() {
            return Collections.unmodifiableSortedSet(invalidIds);
        }

        public RelatednessSet getRelatednessSet() {
            return new RelatednessSet(this.comparisons);
        }
    }

    public static class DisambiguationDetails {

        @Expose
        @Attribute
        @Element(required = false)
        private String term1;
        @Expose
        @Attribute
        @Element(required = false)
        private String term2;

        @Expose
        @Attribute
        private final int term1Candidates;

        @Expose
        @Attribute
        private final int term2Candidates;

        @Expose
        @ElementList(inline = true)
        private final ArrayList<Interpretation> interpretations;

        private DisambiguationDetails(LabelComparer.ComparisonDetails details) {

            term1Candidates = details.getLabelA().getSenses().length;
            term2Candidates = details.getLabelB().getSenses().length;

            interpretations = new ArrayList<Interpretation>();
            for (LabelComparer.SensePair sp : details.getCandidateInterpretations()) {
                interpretations.add(new Interpretation(sp));
            }
        }

        private DisambiguationDetails(LabelComparer.ComparisonDetails details, String term1, String term2) {

            term1Candidates = details.getLabelA().getSenses().length;
            term2Candidates = details.getLabelB().getSenses().length;

            interpretations = new ArrayList<Interpretation>();
            for (LabelComparer.SensePair sp : details.getCandidateInterpretations()) {
                interpretations.add(new Interpretation(sp));
            }
            this.term1 = term1;
            this.term2 = term2;
        }

        public int getTerm1Candidates() {
            return term1Candidates;
        }

        public int getTerm2Candidates() {
            return term2Candidates;
        }

        public List<Interpretation> getInterpretations() {
            return Collections.unmodifiableList(interpretations);
        }
    }

    public static class Interpretation {

        @Expose
        @Attribute
        private int id1;

        @Expose
        @Attribute
        private int id2;

        @Expose
        @Attribute
        private String title1;

        @Expose
        @Attribute
        private String title2;

        @Expose
        @Attribute
        private double relatedness;

        @Expose
        @Attribute
        private double disambiguationConfidence;

        private Interpretation(LabelComparer.SensePair sensePair) {
            id1 = sensePair.getSenseA().getId();
            title1 = sensePair.getSenseA().getTitle();

            id2 = sensePair.getSenseB().getId();
            title2 = sensePair.getSenseB().getTitle();

            relatedness = sensePair.getSenseRelatedness();
            disambiguationConfidence = sensePair.getDisambiguationConfidence();
        }

        public int getId1() {
            return id1;
        }

        public int getId2() {
            return id2;
        }

        public String getTitle1() {
            return title1;
        }

        public String getTitle2() {
            return title2;
        }

        public double getRelatedness() {
            return relatedness;
        }

        public double getDisambiguationConfidence() {
            return disambiguationConfidence;
        }
    }

    public static class Comparison {

        @Expose
        @Attribute
        private int lowId;

        @Expose
        @Attribute(required = false)
        private String lowTitle;

        @Expose
        @Attribute
        private int highId;

        @Expose
        @Attribute(required = false)
        private String highTitle;

        @Expose
        @Attribute
        private double relatedness;

        private Comparison(Article lowArt, Article highArt, double relatedness, boolean showTitles) {
            this.lowId = lowArt.getId();
            this.highId = highArt.getId();

            if (showTitles) {
                lowTitle = lowArt.getTitle();
                highTitle = highArt.getTitle();
            }

            this.relatedness = relatedness;
        }

        public int getLowId() {
            return lowId;
        }

        public String getLowTitle() {
            return lowTitle;
        }

        public int getHighId() {
            return highId;
        }

        public String getHighTitle() {
            return highTitle;
        }

        public double getRelatedness() {
            return relatedness;
        }
    }

    public static class Snippet {

        @Expose
        @Attribute
        private int sourceId;

        @Expose
        @Attribute
        private String sourceTitle;

        @Expose
        @Text(data = true)
        private String markup;

        @Expose
        @Attribute
        private double weight;

        @Expose
        @Attribute
        private int sentenceIndex;

        private Snippet(ConnectionSnippet cs, String markup) {
            this.sourceId = cs.getSource().getId();
            this.sourceTitle = cs.getSource().getTitle();
            this.weight = cs.getWeight();
            this.sentenceIndex = cs.getSentenceIndex();

            this.markup = markup;
        }

        public int getSourceId() {
            return sourceId;
        }

        public String getSourceTitle() {
            return sourceTitle;
        }

        public String getMarkup() {
            return markup;
        }

        public double getWeight() {
            return weight;
        }

        public int getSentenceIndex() {
            return sentenceIndex;
        }
    }

    public static class Connection {

        @Expose
        @Attribute
        private int id;

        @Expose
        @Attribute
        private String title;

        @Expose
        @Attribute
        private double relatedness1;

        @Expose
        @Attribute
        private double relatedness2;

        private Connection(Article art, double relatedness1, double relatedness2) {
            this.id = art.getId();
            this.title = art.getTitle();

            this.relatedness1 = relatedness1;
            this.relatedness2 = relatedness2;
        }

        public int getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public double getRelatedness1() {
            return relatedness1;
        }

        public double getRelatedness2() {
            return relatedness2;
        }
    }

    public static class RelatednessSet {

        HashMap<Long, Double> measures = new HashMap<Long, Double>();

        private Long getKey(int id1, int id2) {
            long min = Math.min(id1, id2);
            long max = Math.max(id1, id2);
            return min + (max << 30);
        }

        public RelatednessSet(List<Comparison> comparisons) {

            for (Comparison c : comparisons) {
                measures.put(getKey(c.getLowId(), c.getHighId()), c.getRelatedness());
            }

        }

        public Double getRelatedness(int id1, int id2) {
            return measures.get(getKey(id1, id2));
        }
    }
}
