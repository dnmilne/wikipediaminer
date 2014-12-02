package org.wikipedia.miner.web.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.Element;
import org.simpleframework.xml.ElementList;
import org.simpleframework.xml.Text;
import org.wikipedia.miner.comparison.ArticleComparer;
import org.wikipedia.miner.model.Article;
import org.wikipedia.miner.model.Category;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.web.util.ImageRetriever;
import org.wikipedia.miner.web.util.UtilityMessages.InvalidTitleMessage;
import org.apache.log4j.Logger;
import org.dmilne.xjsf.Service;
import org.dmilne.xjsf.UtilityMessages.ErrorMessage;
import org.dmilne.xjsf.UtilityMessages.ParameterMissingMessage;
import org.dmilne.xjsf.param.BooleanParameter;
import org.dmilne.xjsf.param.EnumParameter;
import org.dmilne.xjsf.param.IntParameter;
import org.dmilne.xjsf.param.ParameterGroup;
import org.dmilne.xjsf.param.StringParameter;

import com.google.gson.annotations.Expose;
import java.util.AbstractList;
import org.dmilne.xjsf.param.IntListParameter;
import org.wikipedia.miner.web.util.xjsfParameters.StringListParameter;

@SuppressWarnings("serial")
public class ExploreArticleService extends WMService {

    //TODO: modify freebase image request to use article titles rather than ids
    //TODO: if lang is not en, use languageLinks to translate article title to english.
    private enum GroupName {

        id, title, titles, ids
    };

    public enum DefinitionLength {

        LONG, SHORT
    };

    private ImageRetriever imageRetriever;

    private ParameterGroup grpId;
    private IntParameter prmId;

    private ParameterGroup grpIds;
    private IntListParameter prmIds;

    private ParameterGroup grpTitle;
    private StringParameter prmTitle;

    private ParameterGroup grpTitles;
    private StringListParameter prmTitles;

    private BooleanParameter prmDefinition;
    private EnumParameter<DefinitionLength> prmDefinitionLength;

    private BooleanParameter prmLabels;

    private BooleanParameter prmTranslations;

    private BooleanParameter prmImages;
    private IntParameter prmImageWidth;
    private IntParameter prmImageHeight;

    private BooleanParameter prmParentCategories;

    private BooleanParameter prmInLinks;
    private IntParameter prmInLinkMax;
    private IntParameter prmInLinkStart;

    private BooleanParameter prmOutLinks;
    private IntParameter prmOutLinkMax;
    private IntParameter prmOutLinkStart;

    private BooleanParameter prmLinkRelatedness;

    private static final Logger logger = Logger.getLogger(ExploreArticleService.class);

    public ExploreArticleService() {

        super("core", "Provides details of individual articles",
                "<p></p>", false
        );
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        grpId = new ParameterGroup(GroupName.id.name(), "To retrieve an article by  id");
        prmId = new IntParameter("id", "The unique identifier of the article to explore", null);
        grpId.addParameter(prmId);
        addParameterGroup(grpId);

        grpIds = new ParameterGroup(GroupName.ids.name(), "To retrieve articles by  id");
        prmIds = new IntListParameter("ids", "The list of unique identifiers for the articles to explore", null);
        grpIds.addParameter(prmIds);
        addParameterGroup(grpIds);

        grpTitle = new ParameterGroup(GroupName.title.name(), "To retrieve article by title");
        prmTitle = new StringParameter("title", "The (case sensitive) title of the article to explore", null);
        grpTitle.addParameter(prmTitle);
        addParameterGroup(grpTitle);

        grpTitles = new ParameterGroup(GroupName.titles.name(), "To retrieve article by title");
        prmTitles = new StringListParameter("titles", "The (case sensitive) titles of the articles to explore", null);
        grpTitles.addParameter(prmTitle);
        addParameterGroup(grpTitles);

        prmDefinition = new BooleanParameter("definition", "<b>true</b> if a snippet definition should be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmDefinition);

        String[] descLength = {"first paragraph", "first sentence"};
        prmDefinitionLength = new EnumParameter<DefinitionLength>("definitionLength", "The required length of the definition", DefinitionLength.SHORT, DefinitionLength.values(), descLength);
        addGlobalParameter(prmDefinitionLength);

        addGlobalParameter(getWMHub().getFormatter().getEmphasisFormatParam());
        addGlobalParameter(getWMHub().getFormatter().getLinkFormatParam());

        prmLabels = new BooleanParameter("labels", "<b>true</b> if labels (synonyms, etc) for this topic are to be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmLabels);

        prmTranslations = new BooleanParameter("translations", "<b>true</b> if translations (language links) for this topic are to be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmTranslations);

        prmImages = new BooleanParameter("images", "Whether or not to retrieve relevant image urls from freebase", false);
        addGlobalParameter(prmImages);

        prmImageWidth = new IntParameter("maxImageWidth", "Images can be scaled. This defines their maximum width, in pixels", 150);
        addGlobalParameter(prmImageWidth);

        prmImageHeight = new IntParameter("maxImageHeight", "Images can be scaled. This defines their maximum height, in pixels", 150);
        addGlobalParameter(prmImageHeight);

        prmParentCategories = new BooleanParameter("parentCategories", "<b>true</b> if parent categories of this category should be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmParentCategories);

        prmInLinks = new BooleanParameter("inLinks", "<b>true</b> if articles that link to this article should be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmInLinks);

        prmInLinkMax = new IntParameter("inLinkMax", "the maximum number of in-links that should be returned. A max of <b>0</b> will result in all in-links being returned", 250);
        addGlobalParameter(prmInLinkMax);

        prmInLinkStart = new IntParameter("inLinkStart", "the index of the first in-link to return. Combined with <b>inLinkMax</b>, this parameter allows the user to page through large lists of in-links", 0);
        addGlobalParameter(prmInLinkStart);

        prmOutLinks = new BooleanParameter("outLinks", "<b>true</b> if articles that this article links to should be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmOutLinks);

        prmOutLinkMax = new IntParameter("outLinkMax", "the maximum number of out-links that should be returned. A max of <b>0</b> will result in all out-links being returned", 250);
        addGlobalParameter(prmOutLinkMax);

        prmOutLinkStart = new IntParameter("outLinkStart", "the index of the first out-link to return. Combined with <b>outLinkMax</b>, this parameter allows the user to page through large lists of out-links", 0);
        addGlobalParameter(prmOutLinkStart);

        prmLinkRelatedness = new BooleanParameter("linkRelatedness", "<b>true</b> if the relatedness of in- and out-links should be measured, otherwise <b>false</b>", false);
        addGlobalParameter(prmLinkRelatedness);

        imageRetriever = new ImageRetriever(getWMHub().getRetriever());

    }

    @Override
    public Service.Message buildWrappedResponse(HttpServletRequest request) throws Exception {

        Wikipedia wikipedia = getWikipedia(request);

        ArticleComparer artComparer = null;
        if (prmLinkRelatedness.getValue(request)) {
            artComparer = getWMHub().getArticleComparer(this.getWikipediaName(request));
            if (artComparer == null) {
                return new ErrorMessage(request, "Relatedness measures are unavailable for this instance of wikipedia");
            }
        }

        ParameterGroup grp = getSpecifiedParameterGroup(request);

        if (grp == null) {
            return new ParameterMissingMessage(request);
        }

        List<Article> articleList = new ArrayList<Article>();
        List<Integer> invalidList = new ArrayList<Integer>();
        List<Integer> nullList = new ArrayList<Integer>();
        List<String> invalidTitle = new ArrayList<String>();
        switch (GroupName.valueOf(grp.getName())) {

            case id:
                Integer id = prmId.getValue(request);
                org.wikipedia.miner.model.Page page = wikipedia.getPageById(id);
                if (page == null) {
                    nullList.add(id);
                }
                switch (page.getType()) {
                    case disambiguation:
                    case article:
                        //TODO what to do with redirects?
                        articleList.add((Article) page);
                        break;
                    default:
                        invalidList.add(id);
                }
                break;
            case ids:
                List<Integer> validList = new ArrayList<Integer>();
                Integer[] ids = prmIds.getValue(request);
                for (int i = 0; i < ids.length; i++) {
                    Integer integer = ids[i];
                    org.wikipedia.miner.model.Page pageIds = wikipedia.getPageById(integer);
                    if (pageIds == null) {
                        nullList.add(integer);
                    }
                    switch (pageIds.getType()) {
                        case disambiguation:
                        case article:
                            articleList.add((Article) pageIds);
                            break;
                        default:
                            if (pageIds.getType() == org.wikipedia.miner.model.Page.PageType.category) {
                                invalidList.add(integer);
                            } else {
                                nullList.add(integer);
                            }
                    }

                }
                break;

            case title:
                String title = prmTitle.getValue(request);
                Article arti = wikipedia.getArticleByTitle(title);

                if (arti == null) {
                    return new InvalidTitleMessage(request, title);
                } else {
                    articleList.add(arti);
                }
                break;
            case titles:
                String[] titles = prmTitles.getValue(request);
                if (titles == null) {
                    return new ParameterMissingMessage(request);
                }
                for (String title1 : titles) {
                    arti = wikipedia.getArticleByTitle(title1);
                    if (arti != null) {
                        articleList.add(arti);
                    } else {
                        invalidTitle.add(title1);
                    }
                }
                break;
        }

        MessageList msge = new MessageList(request, nullList, invalidList, invalidTitle);
        for (Article art : articleList) {
            ArticleMsg msg = new ArticleMsg(art);

            if (prmDefinition.getValue(request)) {
                String definition = null;

                if (prmDefinitionLength.getValue(request) == DefinitionLength.SHORT) {
                    definition = art.getSentenceMarkup(0);
                } else {
                    definition = art.getFirstParagraphMarkup();
                }

                msg.setDefinition(getWMHub().getFormatter().format(definition, request, wikipedia));
            }

            if (prmLabels.getValue(request)) {
                //get labels for this concept

                Article.Label[] labels = art.getLabels();
                int total = 0;
                for (Article.Label lbl : labels) {
                    total += lbl.getLinkOccCount();
                }

                for (Article.Label lbl : labels) {
                    long occ = lbl.getLinkOccCount();

                    if (occ > 0) {
                        msg.addLabel(new Label(lbl, total));
                    }

                }
            }

            if (prmTranslations.getValue(request)) {
                TreeMap<String, String> translations = art.getTranslations();
                for (Map.Entry<String, String> entry : translations.entrySet()) {
                    msg.addTranslation(new Translation(entry.getKey(), entry.getValue()));
                }
            }

            if (prmImages.getValue(request)) {

                int width = prmImageWidth.getValue(request);
                int height = prmImageHeight.getValue(request);

                try {

                    for (String imgTitle : imageRetriever.getImageTitles(art.getId())) {

                        String imgUrl = imageRetriever.getImageUrl(imgTitle, width, height);

                        if (imgUrl != null) {
                            msg.addImage(new Image(imgUrl));
                        }

                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            if (prmParentCategories.getValue(request)) {
                Category[] parents = art.getParentCategories();

                logger.info("retrieving parents from " + parents.length + " total");

                msg.setTotalParentCategories(parents.length);
                for (Category parent : parents) {
                    msg.addParentCategory(new Page(parent));
                }
            }

            if (prmOutLinks.getValue(request)) {

                int start = prmOutLinkStart.getValue(request);
                int max = prmOutLinkMax.getValue(request);
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                } else {
                    max = max + start;
                }

                Article[] linksOut = art.getLinksOut();
                logger.info("retrieving out links [" + start + "," + max + "] from " + linksOut.length + " total");

                msg.setTotalOutLinks(linksOut.length);
                for (int i = start; i < max && i < linksOut.length; i++) {
                    Page p = new Page(linksOut[i]);
                    if (artComparer != null) {
                        p.setRelatedness(artComparer.getRelatedness(art, linksOut[i]));
                    }

                    msg.addOutLink(p);
                }
            }

            if (prmInLinks.getValue(request)) {

                int start = prmInLinkStart.getValue(request);
                int max = prmInLinkMax.getValue(request);
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                } else {
                    max = max + start;
                }

                Article[] linksIn = art.getLinksIn();
                logger.info("retrieving in links [" + start + "," + max + "] from " + linksIn.length + " total");

                msg.setTotalInLinks(linksIn.length);
                for (int i = start; i < max && i < linksIn.length; i++) {
                    Page p = new Page(linksIn[i]);
                    if (artComparer != null) {
                        p.setRelatedness(artComparer.getRelatedness(art, linksIn[i]));
                    }

                    msg.addInLink(p);
                }
            }
            msge.addArticle(msg);
        }
        return msge;

    }

    public static class MessageList extends Service.Message {

        @Expose
        @ElementList(required = true, entry = "invalidList")
        private List<Integer> invalidList = null;
        @Expose
        @ElementList(required = true, entry = "articleLis")
        private List<ArticleMsg> articleList = null;
        @Expose
        @ElementList(required = true, entry = "nullList")
        private List<Integer> nullList = null;

        @Expose
        @ElementList(required = true, entry = "invalidTitles")
        private List<String> invalidTitles = null;

        private MessageList(HttpServletRequest request, List<Integer> nullList, List<Integer> invalidList, List<String> invalidTitles) {
            super(request);
            this.invalidList = invalidList;
            this.nullList = nullList;
            articleList = new ArrayList<ArticleMsg>();
            this.invalidTitles = invalidTitles;

        }

        private void addArticle(ArticleMsg arti) {
            articleList.add(arti);
        }

        /**
         * @return the invalidTitles
         */
        public List<String> getInvalidTitles() {
            return invalidTitles;
        }

        /**
         * @param invalidTitles the invalidTitles to set
         */
        public void setInvalidTitles(List<String> invalidTitles) {
            this.invalidTitles = invalidTitles;
        }
    }

    public static class ArticleMsg {

        @Expose
        @Attribute
        private final int id;
        @Expose
        @Attribute
        private final String title;
        @Expose
        @Element(required = false, data = true)
        private String definition;
        @Expose
        @ElementList(required = false, entry = "image")
        private ArrayList<Image> images = null;
        @Expose
        @ElementList(required = false, entry = "label")
        private ArrayList<Label> labels = null;
        @Expose
        @ElementList(required = false, entry = "tranlation")
        private ArrayList<Translation> translations = null;
        @Expose
        @ElementList(required = false, entry = "parentCategory")
        private ArrayList<Page> parentCategories = null;
        @Expose
        @Attribute(required = false)
        private Integer totalParentCategories;
        @Expose
        @ElementList(required = false, entry = "inLink")
        private ArrayList<Page> inLinks = null;
        @Expose
        @Attribute(required = false)
        private Integer totalInLinks;
        @Expose
        @ElementList(required = false, entry = "outLink")
        private ArrayList<Page> outLinks = null;
        @Expose
        @Attribute(required = false)
        private Integer totalOutLinks;

        private ArticleMsg(Article art) {
            this.id = art.getId();
            this.title = art.getTitle();
        }

        private void setDefinition(String markup) {
            this.definition = markup;
        }

        private void addImage(Image image) {
            if (images == null) {
                images = new ArrayList<Image>();
            }

            images.add(image);
        }

        private void addLabel(Label label) {
            if (labels == null) {
                labels = new ArrayList<Label>();
            }

            labels.add(label);
        }

        private void addTranslation(Translation t) {
            if (translations == null) {
                translations = new ArrayList<Translation>();
            }

            translations.add(t);
        }

        private void addParentCategory(Page p) {
            if (parentCategories == null) {
                parentCategories = new ArrayList<Page>();
            }

            parentCategories.add(p);
        }

        private void setTotalParentCategories(int total) {
            totalParentCategories = total;
        }

        private void addInLink(Page p) {
            if (inLinks == null) {
                inLinks = new ArrayList<Page>();
            }

            inLinks.add(p);
        }

        private void setTotalInLinks(int total) {
            totalInLinks = total;
        }

        private void addOutLink(Page p) {
            if (outLinks == null) {
                outLinks = new ArrayList<Page>();
            }

            outLinks.add(p);
        }

        private void setTotalOutLinks(int total) {
            totalOutLinks = total;
        }

        public int getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public String getDefinition() {
            return definition;
        }

        public List<Image> getImages() {

            if (images == null) {
                return Collections.unmodifiableList(new ArrayList<Image>());
            }

            return Collections.unmodifiableList(images);
        }

        public List<Label> getLabels() {

            if (labels == null) {
                return Collections.unmodifiableList(new ArrayList<Label>());
            }

            return Collections.unmodifiableList(labels);
        }

        public List<Translation> getTranslations() {

            if (translations == null) {
                return Collections.unmodifiableList(new ArrayList<Translation>());
            }

            return Collections.unmodifiableList(translations);
        }

        public List<Page> getParentCategories() {

            if (parentCategories == null) {
                return Collections.unmodifiableList(new ArrayList<Page>());
            }

            return Collections.unmodifiableList(parentCategories);
        }

        public Integer getTotalParentCategories() {
            return totalParentCategories;
        }

        public List<Page> getInLinks() {

            if (inLinks == null) {
                return Collections.unmodifiableList(new ArrayList<Page>());
            }

            return Collections.unmodifiableList(inLinks);
        }

        public Integer getTotalInLinks() {
            return totalInLinks;
        }

        public List<Page> getOutLinks() {

            if (outLinks == null) {
                return Collections.unmodifiableList(new ArrayList<Page>());
            }

            return Collections.unmodifiableList(outLinks);
        }

        public Integer getTotalOutLinks() {
            return totalOutLinks;
        }
    }

    public static class Image {

        @Expose
        @Attribute
        private final String url;

        private Image(String url) {
            this.url = url;
        }

        public String getUrl() {
            return url;
        }
    }

    public static class Label {

        @Expose
        @Attribute
        private final String text;
        @Expose
        @Attribute
        private final long occurrances;
        @Expose
        @Attribute
        private final double proportion;
        @Expose
        @Attribute
        private final boolean isPrimary;
        @Expose
        @Attribute
        private final boolean fromRedirect;
        @Expose
        @Attribute
        private final boolean fromTitle;

        private Label(Article.Label lbl, long totalOccurrances) {

            text = lbl.getText();
            occurrances = lbl.getLinkOccCount();
            proportion = (double) occurrances / totalOccurrances;
            isPrimary = lbl.isPrimary();
            fromRedirect = lbl.isFromRedirect();
            fromTitle = lbl.isFromTitle();
        }

        public String getText() {
            return text;
        }

        public long getOccurrances() {
            return occurrances;
        }

        public double getProportion() {
            return proportion;
        }

        public boolean isPrimary() {
            return isPrimary;
        }

        public boolean isFromRedirect() {
            return fromRedirect;
        }

        public boolean isFromTitle() {
            return fromTitle;
        }
    }

    public static class Translation {

        @Expose
        @Attribute
        private String lang;
        @Expose
        @Text(data = true)
        private String text;

        private Translation(String lang, String text) {
            this.lang = lang;
            this.text = text;
        }

        public String getLang() {
            return lang;
        }

        public String getText() {
            return text;
        }
    }

    public static class Page {

        @Expose
        @Attribute
        private final int id;
        @Expose
        @Attribute
        private final String title;
        @Expose
        @Attribute(required = false)
        private Double relatedness;

        protected Page(org.wikipedia.miner.model.Page p) {
            this.id = p.getId();
            this.title = p.getTitle();
        }

        protected void setRelatedness(double relatedness) {
            this.relatedness = relatedness;
        }

        public int getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public Double getRelatedness() {
            return relatedness;
        }
    }
}
