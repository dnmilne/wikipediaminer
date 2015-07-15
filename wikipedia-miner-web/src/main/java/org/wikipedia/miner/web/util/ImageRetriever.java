package org.wikipedia.miner.web.util;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.wikipedia.miner.model.Article;
import org.xml.sax.SAXException;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ImageRetriever {

    private static String baseUrl = "https://en.wikipedia.org/w/api.php";
    private static String baseUrlEs = "https://es.wikipedia.org/w/api.php";
    private WebContentRetriever retriever;
    private Gson gson = new Gson();
    private Set<String> bannedImages = new HashSet<String>();

    public ImageRetriever(WebContentRetriever retriever) {

        this.retriever = retriever;

        bannedImages.add("File:Commons-logo.svg");
        bannedImages.add("fileicon-ogg.png");
    }

    public List<String> getImageTitles(Integer articleId, String wikipedia) throws UnsupportedEncodingException, IOException {

        switch (wikipedia) {
            case "es":
                baseUrl = baseUrlEs;
                break;
            default:
                baseUrl = baseUrl;
        }

        List<String> imageTitles = new ArrayList<String>();

        URL url = new URL(baseUrl + "?action=query&pageids=" + articleId + "&prop=images&rawcontinue&format=json");

        String json = retriever.getWebContent(url);

        Response response = gson.fromJson(json, Response.class);

        if (response == null) {
            return imageTitles;
        }

        if (response.query == null) {
            return imageTitles;
        }

        if (response.query.pages == null) {
            return imageTitles;
        }

        for (Page page : response.query.pages.values()) {

            if (page.images == null) {
                continue;
            }

            for (Image image : page.images) {
                if (bannedImages.contains(image.title)) {
                    continue;
                }

                imageTitles.add(image.title);
            }
        }

        return imageTitles;
    }

    public String getImageUrl(String imageTitle, Integer width, Integer height, String wikipedia) throws UnsupportedEncodingException, MalformedURLException, IOException {
        switch (wikipedia) {
            case "es":
                baseUrl = baseUrlEs;
                break;
            default:
                baseUrl = baseUrl;
        }
        String url = baseUrl + "?action=query&titles=" + URLEncoder.encode(imageTitle, "UTF-8") + "&prop=imageinfo&iiprop=url&format=json";

        if (width != null) {
            url = url + "&iiurlwidth=" + width;
        }

        if (height != null) {
            url = url + "&iiurlheight=" + height;
        }

        //System.out.println(url) ;
        String json = retriever.getWebContent(new URL(url));

        //System.out.println(json);
        Response response = gson.fromJson(json, Response.class);

        if (response == null) {
            return null;
        }

        if (response.query == null) {
            return null;
        }

        if (response.query.pages == null) {
            return null;
        }

        for (Page page : response.query.pages.values()) {

            if (page.imageinfo == null) {
                continue;
            }

            for (ImageInfo imageinfo : page.imageinfo) {
                if (imageinfo.thumburl != null) {
                    return imageinfo.thumburl;
                }

                if (imageinfo.url != null) {
                    return imageinfo.url;
                }
            }
        }

        return null;
    }

    public static void main(String args[]) throws ParserConfigurationException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException, SAXException {

        File conf = new File("../configs/hub.xml");
        WebContentRetriever wcr = new WebContentRetriever(new HubConfiguration(conf));
        ImageRetriever ir = new ImageRetriever(wcr);

        for (String img : ir.getImageTitles(852, "en")) {
            System.out.println(img + ": " + ir.getImageUrl(img, 100, null, "en"));
        }
    }

    private static class Response {

        public Query query;
    }

    private static class Query {

        public Map<Integer, Page> pages;
    }

    private static class Page {

        public int pageid;
        public int ns;
        public String title;

        public List<Image> images;
        public List<ImageInfo> imageinfo;
    }

    private static class Image {

        public int ns;
        public String title;
    }

    private static class ImageInfo {

        public String thumburl;
        public int thumbwidth;
        public int thumbheight;
        public String url;
    }

}
