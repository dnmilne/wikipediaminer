package org.wikipedia.miner.web.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

import org.simpleframework.xml.Attribute;
import org.simpleframework.xml.ElementList;
import org.wikipedia.miner.model.Article;
import org.wikipedia.miner.model.Category;
import org.wikipedia.miner.model.Wikipedia;
import org.wikipedia.miner.web.service.ExploreArticleService.Page;
import org.wikipedia.miner.web.util.UtilityMessages.*;
import org.dmilne.xjsf.Service;
import org.dmilne.xjsf.param.BooleanParameter;
import org.dmilne.xjsf.param.IntParameter;
import org.dmilne.xjsf.param.ParameterGroup;
import org.dmilne.xjsf.param.StringParameter;
import org.dmilne.xjsf.UtilityMessages.*;

import com.google.gson.annotations.Expose;
import org.dmilne.xjsf.param.IntListParameter;

@SuppressWarnings("serial")
public class ExploreCategoryService extends WMService {

    private enum GroupName {

        id, title, ids
    };

    private ParameterGroup grpId;
    private IntParameter prmId;

    private ParameterGroup grpIds;
    private IntListParameter prmIds;
    private ParameterGroup grpTitle;
    private StringParameter prmTitle;

    private BooleanParameter prmParentCategories;

    private BooleanParameter prmChildCategories;
    private IntParameter prmChildCategoryMax;
    private IntParameter prmChildCategoryStart;

    private BooleanParameter prmChildArticles;
    private IntParameter prmChildArticleMax;
    private IntParameter prmChildArticleStart;

    public ExploreCategoryService() {
        super("core", "Provides details of individual categories",
                "<p></p>", false
        );
    }

    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);

        grpId = new ParameterGroup(GroupName.id.name(), "To retrieve a category by id");
        prmId = new IntParameter("id", "The unique identifier of the category to explore", null);
        grpId.addParameter(prmId);
        addParameterGroup(grpId);

        grpIds = new ParameterGroup(ExploreCategoryService.GroupName.ids.name(), "To retrieve categories by id");
        prmIds = new IntListParameter("ids", "The list of unique identifiers for the articles to explore", null);
        grpIds.addParameter(prmIds);
        addParameterGroup(grpIds);

        grpTitle = new ParameterGroup(GroupName.title.name(), "To retrieve category by title");
        prmTitle = new StringParameter("title", "The (case sensitive) title of the category to explore", null);
        grpTitle.addParameter(prmTitle);
        addParameterGroup(grpTitle);

        prmParentCategories = new BooleanParameter("parentCategories", "<b>true</b> if parent categories of this category should be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmParentCategories);

        prmChildCategories = new BooleanParameter("childCategories", "<b>true</b> if child categories of this category should be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmChildCategories);

        prmChildCategoryMax = new IntParameter("childCategoryMax", "the maximum number of child categories that should be returned. A max of <b>0</b> will result in all child categories being returned", 250);
        addGlobalParameter(prmChildCategoryMax);

        prmChildCategoryStart = new IntParameter("childCategoryStart", "the index of the first child category to return. Combined with <b>childCategoryMax</b>, this parameter allows the user to page through large lists of child categories", 0);
        addGlobalParameter(prmChildCategoryStart);

        prmChildArticles = new BooleanParameter("childArticles", "<b>true</b> if child articles of this category should be returned, otherwise <b>false</b>", false);
        addGlobalParameter(prmChildArticles);

        prmChildArticleMax = new IntParameter("childArticleMax", "the maximum number of child articles that should be returned. A max of <b>0</b> will result in all child articles being returned", 250);
        addGlobalParameter(prmChildArticleMax);

        prmChildArticleStart = new IntParameter("childArticleStart", "the index of the first child article to return. Combined with <b>childArticleMax</b>, this parameter allows the user to page through large lists of child articles", 0);
        addGlobalParameter(prmChildArticleStart);

    }

    @Override
    public Service.Message buildWrappedResponse(HttpServletRequest request) throws Exception {

        Wikipedia wikipedia = getWikipedia(request);

        ParameterGroup grp = getSpecifiedParameterGroup(request);

        if (grp == null) {
            return new ParameterMissingMessage(request);
        }

        Category cat = null;
        List<Category> list = new ArrayList<Category>();
        List<Integer> invalidlist = new ArrayList<Integer>();
        List<Integer> nulllist = new ArrayList<Integer>();
        switch (GroupName.valueOf(grp.getName())) {
 case id:
                Integer id = prmId.getValue(request);

                org.wikipedia.miner.model.Page page = wikipedia.getPageById(id);
                if (page == null) {
                    nulllist.add(id);
                }

                switch (page.getType()) {
                    case category:
                        list.add((Category) page);
                        break;
                    default:

                        invalidlist.add(id);
                }
                break;
            case title:
                String title = prmTitle.getValue(request);
                cat = wikipedia.getCategoryByTitle(title);

                if (cat == null) {
                    return new InvalidTitleMessage(request, title);
                } else {
                    list.add(cat);
                }
                break;
            case ids:
                Integer[] ids = prmIds.getValue(request);
                for (int i = 0; i < ids.length; i++) {
                    Integer integer = ids[i];
                    org.wikipedia.miner.model.Page pageIds = wikipedia.getPageById(integer);
                    if (pageIds == null) {
                        nulllist.add(integer);
                    }
                    switch (pageIds.getType()) {
                        case category:
                            list.add((Category) pageIds);
                            break;
                        default:
                            if (pageIds.getType() == org.wikipedia.miner.model.Page.PageType.article) {
                                invalidlist.add(integer);
                            } else {
                                nulllist.add(integer);
                            }
                    }
                }
                break;
        }

        Message msg = new Message(request, invalidlist, nulllist);
        for (Category catego : list) {

            CatMsg mes = new CatMsg(catego);
            if (prmParentCategories.getValue(request)) {

                Category[] parents = catego.getParentCategories();

                mes.setTotalParentCategories(parents.length);

                for (Category parent : parents) {
                    mes.addParentCategory(new Page(parent));
                }
            }

            if (prmChildCategories.getValue(request)) {

                int start = prmChildCategoryStart.getValue(request);
                int max = prmChildCategoryMax.getValue(request);
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                } else {
                    max = max + start;
                }

                Category[] children = catego.getChildCategories();

                mes.setTotalChildCategories(children.length);
                for (int i = start; i < max && i < children.length; i++) {
                    mes.addChildCategory(new Page(children[i]));
                }
            }

            if (prmChildArticles.getValue(request)) {

                int start = prmChildArticleStart.getValue(request);
                int max = prmChildArticleMax.getValue(request);
                if (max <= 0) {
                    max = Integer.MAX_VALUE;
                } else {
                    max = max + start;
                }

                Article[] children = catego.getChildArticles();

                mes.setTotalChildArticles(children.length);
                for (int i = start; i < max && i < children.length; i++) {
                    mes.addChildArticle(new Page(children[i]));
                }
            }
            msg.addCategory(mes);
        }
        return msg;
    }

public static class Message extends Service.Message {

        @Expose
        @ElementList(required = true, entry = "nullList")
        private List<Integer> nullList = null;
        @Expose
        @ElementList(required = true, entry = "invalidList")
        private List<Integer> invalidList = null;
        @Expose
        @ElementList(required = true, entry = "catList")
        private List<CatMsg> catList = null;
        private ArrayList<Page> childCategories = null;

        private Message(HttpServletRequest request, List<Integer> invalidList, List<Integer> nullList) {
            super(request);
            this.nullList = nullList;
            this.invalidList = invalidList;
            catList = new ArrayList<CatMsg>();
        }

        private void addCategory(CatMsg mes) {
            catList.add(mes);
        }
    }

    public static class CatMsg {

        @Expose
        @Attribute
        private int id;
        @Expose
        @Attribute
        private String title;
        @Expose
        @ElementList(required = false, entry = "parentCategory")
        private ArrayList<Page> parentCategories = null;
        @Expose
        @Attribute(required = false)
        private Integer totalParentCategories;
        @Expose
        @ElementList(required = false, entry = "childCategory")
        private ArrayList<Page> childCategories = null;
        @Expose
        @Attribute(required = false)
        private Integer totalChildCategories;
        @Expose
        @ElementList(required = false, entry = "childArticle")
        private ArrayList<Page> childArticles = null;
        @Expose
        @Attribute(required = false)
        private Integer totalChildArticles;

        private CatMsg(Category cat) {
            this.id = cat.getId();
            this.title = cat.getTitle();
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

        private void addChildCategory(Page p) {
            if (childCategories == null) {
                childCategories = new ArrayList<Page>();
            }

            childCategories.add(p);
        }

        private void setTotalChildCategories(int total) {
            totalChildCategories = total;
        }

        private void addChildArticle(Page p) {
            if (childArticles == null) {
                childArticles = new ArrayList<Page>();
            }

            childArticles.add(p);
        }

        private void setTotalChildArticles(int total) {
            totalChildArticles = total;
        }

        public int getId() {
            return id;
        }

        public String getTitle() {
            return title;
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

        public List<Page> getChildCategories() {
            if (childCategories == null) {
                return Collections.unmodifiableList(new ArrayList<Page>());
            }

            return Collections.unmodifiableList(childCategories);
        }

        public Integer getTotalChildCategories() {
            return totalChildCategories;
        }

        public List<Page> getChildArticles() {
            if (childArticles == null) {
                return Collections.unmodifiableList(new ArrayList<Page>());
            }

            return Collections.unmodifiableList(childArticles);
        }

        public Integer getTotalChildArticles() {
            return totalChildArticles;
        }
    }
}
