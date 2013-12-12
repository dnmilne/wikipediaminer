package org.wikipedia.miner.web.service;

import gnu.trove.map.hash.TIntFloatHashMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import opennlp.tools.util.Span;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.wikipedia.miner.comparison.ArticleComparer;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.util.NGrammer;
import org.wikipedia.miner.util.NGrammer.NGramSpan;
import org.wikipedia.miner.util.RelatednessCache;
import org.dmilne.xjsf.Service;
import org.dmilne.xjsf.UtilityMessages.ErrorMessage;
import org.dmilne.xjsf.UtilityMessages.ParameterMissingMessage;
import org.dmilne.xjsf.param.BooleanParameter;
import org.dmilne.xjsf.param.FloatParameter;
import org.dmilne.xjsf.param.StringParameter;

import com.google.gson.annotations.Expose;
import opennlp.tools.util.StringList;
import org.wikipedia.miner.web.util.xjsfParameters.StringListParameter;

/**
 *
 *
 *
 * NOTE: this does not support {@link Service.ResponseFormat#DIRECT}
 */
public class SearchService extends WMService {

    private static final long serialVersionUID = 5011451347638265017L;

    Pattern topicPattern = Pattern.compile("\\[\\[(\\d+)\\|(.*?)\\]\\]");
    Pattern quotePattern = Pattern.compile("\"(.*?)\"");

    private StringParameter prmQuery;
    private BooleanParameter prmComplex;
    private FloatParameter prmMinPriorProb;
    private StringListParameter prmQueryList;

    public SearchService() {
        super("core", "Lists the senses (wikipedia articles) of terms and phrases",
                "<p>This service takes a term or phrase, and returns the different Wikipedia articles that these could refer to.</p>"
                + "<p>By default, it will treat the entire query as one term, but it can be made to break it down into its components "
                + "(to recognize, for example, that <i>hiking new zealand</i> contains two terms: <i>hiking</i> and <i>new zealand</i>)</p>"
                + "<p>For each component term, the service will list the different articles (or concepts) that it could refer to, in order of prior probability "
                + "so that the most obvious senses are listed first.</p>"
                + "<p>For queries that contain multiple terms, the senses of each term will be compared against each other to disambiguate them. This "
                + "provides the weight attribute, which is larger for senses that are likely to be the correct interpretation of the query.</p>", false);
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        prmQuery = new StringParameter("query", "Your query", null);
        addGlobalParameter(prmQuery);

        prmQueryList = new StringListParameter("queryList", "The simple queries you want to run using ',' as separator", null);

        prmComplex = new BooleanParameter("complex", "<b>true</b> if your query might reference multiple topics, otherwise <b>false</b>", false);
        addGlobalParameter(prmComplex);

        prmMinPriorProb = new FloatParameter("minPriorProbability", "the minimum prior probability that a sense must have for it to be returned", 0.01F);
        addGlobalParameter(prmMinPriorProb);

        addExample(
                new ExampleBuilder("List senses of an ambiguous term").
                addParam(prmQuery, "kiwi").
                build()
        );

        addExample(
                new ExampleBuilder("Break a complex multi-topic query into its component terms, and list thier senses").
                addParam(prmQuery, "hiking new zealand").
                addParam(prmComplex, true).
                build()
        );
    }

    @Override
    public Service.Message buildWrappedResponse(HttpServletRequest request) throws Exception {

        String query = prmQuery.getValue(request);
        String[] queryList = prmQueryList.getValue(request);

        if (query == null) {
            if (queryList == null) 
                return new ParameterMissingMessage(request);
             else {
                if (queryList.length > 1)
                return resolveQueryList(queryList, request);
            }
        }else{
       
     
            
            if (prmComplex.getValue(request)) 
                return resolveComplexQuery(query, request);
             else 
                return resolveSimpleQuery(query, request);
            
        }
        return new ParameterMissingMessage(request);
    }

    public Service.Message resolveSimpleQuery(String query, HttpServletRequest request) {

        Wikipedia wikipedia = getWikipedia(request);

        NGrammer nGrammer = new NGrammer(wikipedia.getConfig().getSentenceDetector(), wikipedia.getConfig().getTokenizer());

        NGramSpan span = nGrammer.ngramPosDetect(query)[0];

        org.wikipedia.miner.model.Label label = wikipedia.getLabel(span, query);
        Label rLabel = new Label(label);

        float minPriorProb = prmMinPriorProb.getValue(request);
        for (org.wikipedia.miner.model.Label.Sense sense : label.getSenses()) {

            if (sense.getPriorProbability() < minPriorProb) {
                break;
            }

            rLabel.addSense(new Sense(sense));
        }

        Message msg = new Message(request);
        msg.addLabel(rLabel);

        return msg;
    }

    private Message resolveQueryList(String[] queryList, HttpServletRequest request) {

        Wikipedia wikipedia = getWikipedia(request);

        NGrammer nGrammer = new NGrammer(wikipedia.getConfig().getSentenceDetector(), wikipedia.getConfig().getTokenizer());

        Message msg = new Message(request);
        float minPriorProb = prmMinPriorProb.getValue(request);
        for (String query : queryList) {
            NGramSpan span = nGrammer.ngramPosDetect(query)[0];
            org.wikipedia.miner.model.Label label = wikipedia.getLabel(span, query);
            Label rLabel = new Label(label);
            for (org.wikipedia.miner.model.Label.Sense sense : label.getSenses()) {

                if (sense.getPriorProbability() < minPriorProb) {
                    break;
                }
                rLabel.addSense(new Sense(sense));
            }
            msg.addLabel(rLabel);
        }
        return msg;
    }

    public Service.Message resolveComplexQuery(String query, HttpServletRequest request) throws Exception {

        Wikipedia wikipedia = getWikipedia(request);

        ArticleComparer artComparer = getWMHub().getArticleComparer(getWikipediaName(request));
        if (artComparer == null) {
            return new ErrorMessage(request, "article comparisons are not available with this wikipedia instance");
        }

        ExhaustiveDisambiguator disambiguator = new ExhaustiveDisambiguator(artComparer);

        float minPriorProb = prmMinPriorProb.getValue(request);

        Message msg = new Message(request);

        //resolve query
        ArrayList<QueryLabel> queryLabels = getReferences(query, wikipedia);
        queryLabels = resolveCollisions(queryLabels);
        queryLabels = disambiguator.disambiguate(queryLabels, minPriorProb);

        for (QueryLabel queryLabel : queryLabels) {

            Label rLabel = new Label(queryLabel);

            for (org.wikipedia.miner.model.Label.Sense sense : queryLabel.getLabel().getSenses()) {

                if (sense.getPriorProbability() < minPriorProb) {
                    break;
                }

                Sense rSense = new Sense(sense);
                rSense.setWeight(disambiguator.getSenseWeight(sense.getId()));
                if (queryLabel.getSelectedSenseId() != null && queryLabel.getSelectedSenseId() == sense.getId()) {
                    rSense.setIsSelected(true);
                }

                rLabel.addSense(rSense);
            }
            rLabel.sortSensesByWeight();
            msg.addLabel(rLabel);
        }
        return msg;
    }

    private ArrayList<QueryLabel> getReferences(String query, Wikipedia wikipedia) {

        ArrayList<QueryLabel> queryLabels = new ArrayList<QueryLabel>();

        NGrammer nGrammer = new NGrammer(wikipedia.getConfig().getSentenceDetector(), wikipedia.getConfig().getTokenizer());

        //spans that can't be interrupted or intersected
        ArrayList<Span> contiguousSpans = new ArrayList<Span>();

        //spans that have already been disambiguated
        HashMap<Long, Integer> topicIdsBySpan = new HashMap<Long, Integer>();

        String cleanedQuery = cleanTopicMarkup(query, contiguousSpans, topicIdsBySpan);
        cleanedQuery = cleanQuoteMarkup(cleanedQuery, contiguousSpans);
        Collections.sort(contiguousSpans);

        //System.out.println("Cleaned query: " + cleanedQuery) ;
        for (NGramSpan span : nGrammer.ngramPosDetect(cleanedQuery)) {

            //System.out.println("  ngram: " + span.getCoveredText(cleanedQuery) + " " + span.getStart() + ", " + span.getEnd()) ;
            if (!isSpanValid(span, contiguousSpans)) {
                //System.out.println("   invalid") ;
                continue;
            }

            Integer topicId = topicIdsBySpan.get(getKey(span));

            org.wikipedia.miner.model.Label lbl = wikipedia.getLabel(span, cleanedQuery);
            QueryLabel ql = new QueryLabel(lbl, wikipedia.getConfig().isStopword(span.getNgram(cleanedQuery)), span, topicId);

            queryLabels.add(ql);
            //System.out.println("   lp: " + label.getLinkProbability()) ;
        }

        return queryLabels;
    }

    private ArrayList<QueryLabel> resolveCollisions(ArrayList<QueryLabel> queryLabels) {

        for (int i = 0; i < queryLabels.size(); i++) {
            QueryLabel lbl1 = queryLabels.get(i);

            List<QueryLabel> overlappingTopics = new ArrayList<QueryLabel>();

            double qtWeight = lbl1.getWeight();

            double overlapWeight = 0;

            for (int j = i + 1; j < queryLabels.size(); j++) {
                QueryLabel lbl2 = queryLabels.get(j);

                //TODO: contains might not be right
                if (lbl1.getSpan().intersects(lbl2.getSpan())) {
                    overlappingTopics.add(lbl2);

                    if (!lbl2.isStopword) {
                        overlapWeight = overlapWeight + lbl2.getWeight();
                    }
                } else {
                    break;
                }
            }

            if (overlappingTopics.size() > 0) {
                overlapWeight = overlapWeight / overlappingTopics.size();
            }

            if (overlapWeight > qtWeight) {
                // want to keep the overlapped items
                queryLabels.remove(i);
                i = i - 1;
            } else {
                //want to keep the overlapping item
                for (int j = 0; j < overlappingTopics.size(); j++) {
                    queryLabels.remove(i + 1);
                }
            }
        }

        return queryLabels;
    }

    private Long getKey(Span s) {
        long key = s.getStart() + (s.getEnd() << 30);
        return key;
    }

    private boolean isSpanValid(Span span, ArrayList<Span> contiguousSpans) {

        for (Span s : contiguousSpans) {

            if (s.equals(span)) {
                return true;
            }

            if (s.intersects(span) || s.crosses(span) || s.contains(span) || span.contains(s)) {
                return false;
            }

            if (s.getStart() > span.getEnd()) {
                break;
            }
        }

        return true;

    }

    private String cleanTopicMarkup(String query, ArrayList<Span> contiguousSpans, HashMap<Long, Integer> topicIdsBySpan) {

        StringBuilder sb = new StringBuilder();

        int lastCopyPoint = 0;
        Matcher m = topicPattern.matcher(query);

        while (m.find()) {
            sb.append(query.substring(lastCopyPoint, m.start()));

            Span span = new Span(sb.length(), sb.length() + m.group(2).length());

            contiguousSpans.add(span);
            topicIdsBySpan.put(getKey(span), Integer.parseInt(m.group(1)));

            sb.append(m.group(2));
            lastCopyPoint = m.end();
        }

        sb.append(query.substring(lastCopyPoint));
        return sb.toString();
    }

    private String cleanQuoteMarkup(String query, ArrayList<Span> contiguousSpans) {
        StringBuilder sb = new StringBuilder();

        int lastCopyPoint = 0;
        Matcher m = quotePattern.matcher(query);

        while (m.find()) {
            sb.append(query.substring(lastCopyPoint, m.start()));

            Span span = new Span(sb.length(), sb.length() + m.group(1).length());

            contiguousSpans.add(span);

            sb.append(m.group(1));
            lastCopyPoint = m.end();
        }

        sb.append(query.substring(lastCopyPoint));
        return sb.toString();

    }

    public class ExhaustiveDisambiguator {

        //TODO: make this use disambiguator in labelComparer instead.
        ArrayList<QueryLabel> queryTerms;
        RelatednessCache rc;

        Integer[] selectedSenses;
        org.wikipedia.miner.model.Label.Sense currCombo[];
        //org.wikipedia.miner.model.Label.Sense bestCombo[] ;
        float bestComboWeight;

        private TIntFloatHashMap bestSenseWeights;

        public ExhaustiveDisambiguator(ArticleComparer comparer) {

            rc = new RelatednessCache(comparer);

        }

        public ArrayList<QueryLabel> disambiguate(ArrayList<QueryLabel> queryTerms, float minPriorProb) throws Exception {

            this.queryTerms = queryTerms;

            this.currCombo = new org.wikipedia.miner.model.Label.Sense[queryTerms.size()];

            //this.bestCombo = null ;
            this.bestComboWeight = 0;

            this.selectedSenses = new Integer[queryTerms.size()];
            for (int i = 0; i < queryTerms.size(); i++) {
                this.selectedSenses[i] = queryTerms.get(i).selectedSenseId;
            }

            bestSenseWeights = new TIntFloatHashMap();

            //recursively check and weight every possible combination of senses
            checkSenses(0, minPriorProb);

            return queryTerms;
        }

        public float getSenseWeight(int id) {
            return bestSenseWeights.get(id);
        }

        private void checkSenses(int termIndex, float minPriorProb) throws Exception {

            if (termIndex == queryTerms.size()) {

                //this is a complete (and unique) combination of senses, so lets weight it
                weightCombo();
            } else {
                // this is not a complete combination of senses, so continue recursion
                QueryLabel qt = queryTerms.get(termIndex);

                if (qt.isStopword || qt.getLabel().getSenses().length == 0) {
                    checkSenses(termIndex + 1, minPriorProb);
                } else {

                    for (org.wikipedia.miner.model.Label.Sense s : qt.getLabel().getSenses()) {

                        if (s.getPriorProbability() < minPriorProb) {
                            break;
                        }

                        currCombo[termIndex] = s;
                        checkSenses(termIndex + 1, minPriorProb);
                    }
                }
            }
        }

        private void weightCombo() throws Exception {

            float commoness = 0;
            float relatedness = 0;
            int comparisons = 0;

            for (int i = 0; i < currCombo.length; i++) {

                if (currCombo[i] != null) {

                    commoness += currCombo[i].getPriorProbability();

                    for (int j = 0; j < currCombo.length; j++) {
                        if (currCombo[j] != null && i != j) {

                            if (selectedSenses[j] == null || currCombo[j].getId() == selectedSenses[j]) {
                                relatedness += rc.getRelatedness(currCombo[i], currCombo[j]);
                            }

                            comparisons++;
                        }
                    }
                }

                i++;
            }

            //average commonness and relatedness
            commoness = commoness / currCombo.length;
            if (comparisons == 0) {
                relatedness = (float) 0.5;
            } else {
                relatedness = relatedness / comparisons;
            }

            //relatedness is three times as important as commonness (hmmm, ad-hoc)
            float weight = (commoness + (3 * relatedness)) / 4;

            //check if this is best overall combination
            if (weight > bestComboWeight) {
                bestComboWeight = weight;
                //bestCombo = currCombo.clone() ;
            }

            //check if this is best weight for each individual sense
            for (org.wikipedia.miner.model.Label.Sense s : currCombo) {
                if (s != null) {
                    double sWeight = bestSenseWeights.get(s.getId());
                    if (sWeight < weight) {
                        bestSenseWeights.put(s.getId(), weight);
                    }
                }
            }
        }
    }

    public class QueryLabel {

        private org.wikipedia.miner.model.Label label;
        private Span span;
        private boolean isStopword;
        private Integer selectedSenseId;

        public QueryLabel(org.wikipedia.miner.model.Label label, boolean isStopword, Span span, Integer selectedSenseId) {

            this.label = label;
            this.isStopword = isStopword;
            this.span = span;
            this.selectedSenseId = selectedSenseId;
        }

        /**
         * @return true if this overlaps the given reference, otherwise false.
         */
        //public boolean overlaps(QueryLabel qt) {
        //	return position.overlaps(qt.getPosition()) ;
        //}
        public org.wikipedia.miner.model.Label getLabel() {
            return label;
        }

        /**
         * @return the position (start and end character locations) in the
         * document where this reference was found.
         */
        public Span getSpan() {
            return span;
        }

        public Integer getSelectedSenseId() {
            return selectedSenseId;
        }

        public double getWeight() {

            if (isStopword) {
                return 0;
            } else if (selectedSenseId != null) {
                return 1;
            } else {
                return label.getLinkProbability();
            }
        }

        /*
         @Override
         public int compareTo(QueryLabel qt) {

         //starts first, then goes first
         int c = new Integer(span.getStart()).compareTo(qt.getSpan().getStart()) ;
         if (c != 0) return c ;

         //starts at same time, so longest one goes first
         c = new Integer(qt.getSpan().getEnd()).compareTo(span.getEnd()) ;
         return c ;
         }*/
    }

    public static class Message extends Service.Message {

        @Expose
        @ElementList(entry = "label", inline = true)
        private ArrayList<Label> labels = new ArrayList<Label>();

        private Message(HttpServletRequest request) {
            super(request);
        }

        private void addLabel(Label lbl) {
            labels.add(lbl);
        }

        public List<Label> getLabels() {
            return Collections.unmodifiableList(labels);
        }

    }

    public static class Label {

        @Expose
        @Attribute
        private final String text;

        @Expose
        @Attribute
        private final long linkDocCount;

        @Expose
        @Attribute
        private final long linkOccCount;

        @Expose
        @Attribute
        private final long docCount;

        @Expose
        @Attribute
        private final long occCount;

        @Expose
        @Attribute
        private final double linkProbability;

        @Expose
        @Attribute(required = false)
        private Boolean isStopword;

        //@Expose
        //@Attribute(required = false)
        //private Integer start ;
        //@Expose
        //@Attribute(required = false)
        //private Integer end ;
        @Expose
        @ElementList(entry = "sense")
        private final ArrayList<Sense> senses;

        private Label(org.wikipedia.miner.model.Label lbl) {

            text = lbl.getText();
            linkDocCount = lbl.getLinkDocCount();
            linkOccCount = lbl.getLinkOccCount();
            docCount = lbl.getDocCount();
            occCount = lbl.getOccCount();
            linkProbability = lbl.getLinkProbability();

            senses = new ArrayList<Sense>();
        }

        private Label(QueryLabel lbl) {

            text = lbl.getLabel().getText();
            linkDocCount = lbl.getLabel().getLinkDocCount();
            linkOccCount = lbl.getLabel().getLinkOccCount();
            docCount = lbl.getLabel().getDocCount();
            occCount = lbl.getLabel().getOccCount();
            linkProbability = lbl.getLabel().getLinkProbability();

            this.isStopword = lbl.isStopword;
            //this.start = lbl.position.getStart() ;
            //this.end = lbl.position.getEnd() ;

            senses = new ArrayList<Sense>();
        }

        private void addSense(Sense s) {
            senses.add(s);
        }

        private void sortSensesByWeight() {

            Collections.sort(senses, new Comparator<Sense>() {

                @Override
                public int compare(Sense s1, Sense s2) {

                    int cmp = 0;

                    if (s1.weight != null && s2.weight != null) {
                        cmp = s2.weight.compareTo(s1.weight);
                    }

                    if (cmp != 0) {
                        return cmp;
                    }

                    cmp = s2.priorProbability.compareTo(s1.priorProbability);

                    if (cmp != 0) {
                        return cmp;
                    }

                    return s1.id.compareTo(s2.id);
                }

            });
        }

        public String getText() {
            return text;
        }

        public long getLinkDocCount() {
            return linkDocCount;
        }

        public long getLinkOccCount() {
            return linkOccCount;
        }

        public long getDocCount() {
            return docCount;
        }

        public long getOccCount() {
            return occCount;
        }

        public double getLinkProbability() {
            return linkProbability;
        }

        public Boolean getIsStopword() {
            return isStopword;
        }

        public ArrayList<Sense> getSenses() {
            return senses;
        }
    }

    public static class Sense {

        @Expose
        @Attribute
        private Integer id;

        @Expose
        @Attribute
        private final String title;

        @Expose
        @Attribute
        private final long linkDocCount;

        @Expose
        @Attribute
        private final long linkOccCount;

        @Expose
        @Attribute
        private Double priorProbability;

        @Expose
        @Attribute
        private final boolean fromTitle;

        @Expose
        @Attribute
        private final boolean fromRedirect;

        @Expose
        @Attribute
        private boolean isSelected;

        @Expose
        @Attribute(required = false)
        private Double weight;

        private Sense(org.wikipedia.miner.model.Label.Sense sense) {

            id = sense.getId();
            title = sense.getTitle();
            linkDocCount = sense.getLinkDocCount();
            linkOccCount = sense.getLinkOccCount();
            priorProbability = sense.getPriorProbability();
            fromTitle = sense.isFromTitle();
            fromRedirect = sense.isFromTitle();
            isSelected = false;
        }

        private void setIsSelected(boolean val) {
            isSelected = val;
        }

        private void setWeight(double weight) {
            this.weight = weight;
        }

        public Integer getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public long getLinkDocCount() {
            return linkDocCount;
        }

        public long getLinkOccCount() {
            return linkOccCount;
        }

        public Double getPriorProbability() {
            return priorProbability;
        }

        public boolean isFromTitle() {
            return fromTitle;
        }

        public boolean isFromRedirect() {
            return fromRedirect;
        }

        public Double getWeight() {
            return weight;
        }

        public boolean isSelected() {
            return isSelected;
        }
    }
}
