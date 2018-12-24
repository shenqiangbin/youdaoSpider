package youdaoSpider;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.zaxxer.hikari.HikariDataSource;

public class QuartzDemoJob implements Job {

    private Logger log = LoggerFactory.getLogger(QuartzDemoJob.class);

    public void execute(JobExecutionContext arg0) throws JobExecutionException {

        print(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        log.info(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));

        String content = getData();
        System.out.println(content);
        getcontentData(content);
    }

    private String getData() {

        String uri = "http://dict.youdao.com/infoline/style?client=deskdict&lastId=0&style=daily&_=";
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet httpget = new HttpGet(uri);

        try {
            CloseableHttpResponse response = client.execute(httpget);
            HttpEntity entity = response.getEntity();

            String content = EntityUtils.toString(entity);
            EntityUtils.consume(entity);
            client.close();

            return content;

        } catch (ClientProtocolException e) {
            log.error(e.getMessage() + e.getStackTrace());
            e.printStackTrace();
        } catch (IOException e) {
            log.error(e.getMessage() + e.getStackTrace());
            e.printStackTrace();
        }

        return "";
    }

    private void getcontentData(String content) {
        JSONObject obj = JSON.parseObject(content);
        JSONArray arr = obj.getJSONArray("cards");

        arr = handleArr(arr);

        for (int i = 0; i < arr.size(); i++) {
            JSONObject card = arr.getJSONObject(i);

            BigInteger id = card.getBigInteger("id");
            String image = card.getString("image");
            String summary = card.getString("summary");
            String title = card.getString("title");
            String videourl = card.getString("videourl");
            String voice = card.getString("voice");

            String videoVal = "";
            if (videourl != null && videourl.contains("mp4"))
                videoVal = videourl;

            String voiceVal = "";
            if (voice != null)
                voiceVal = voice;

            if (com.mysql.jdbc.StringUtils.isNullOrEmpty(image))
                continue;

            String sql = "insert dailyEnglish(id,image,summary,title,addTime,video,voice) values (NULL,?,?,?,?,?,?)";
            String time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
            List<Object> params = new ArrayList<Object>(Arrays.asList(image, summary, title, time, videoVal, voiceVal));

//			StringBuilder builder = new StringBuilder();
//			builder.append("insert dailyEnglish(id,image,summary,title,addTime) values (");
//			builder.append(id).append(",");
//			builder.append("'").append(image.replace("'","\'")).append("',");
//			builder.append("'").append(summary).append("',");
//			builder.append("'").append(title).append("',");
//			builder.append("'").append().append("'");
//			builder.append(")");

            //System.out.println(builder.toString());

            try {
                executeSql(sql, params);
            } catch (SQLException e) {
                log.error(e.getMessage() + e.getStackTrace());
                e.printStackTrace();
            }
        }
    }

    private JSONArray handleArr(JSONArray arr) {

        JSONArray result = new JSONArray();

        for (int i = 0; i < arr.size(); i++) {

            JSONObject card = arr.getJSONObject(i);

            BigInteger id = card.getBigInteger("id");
            String image = card.getString("image");
            String summary = card.getString("summary");
            String title = card.getString("title");
            String videourl = card.getString("videourl");
            String voice = card.getString("voice");

            JSONObject theItem = getItem(result, title);
            if (theItem == null) {
                result.add(card);
            } else {
                String imageVal = com.mysql.jdbc.StringUtils.isNullOrEmpty(theItem.getString("image")) ?
                        image : theItem.getString("image");
                String videoVal = com.mysql.jdbc.StringUtils.isNullOrEmpty(theItem.getString("videourl")) ?
                        videourl : theItem.getString("videourl");
                String voiceVal = com.mysql.jdbc.StringUtils.isNullOrEmpty(theItem.getString("voice")) && theItem.getString("voice").contains("mp3") ?
                        theItem.getString("voice") : voice;

                if(com.mysql.jdbc.StringUtils.isNullOrEmpty(voiceVal))
                    voiceVal = theItem.getString("voice");
                if(com.mysql.jdbc.StringUtils.isNullOrEmpty(voiceVal))
                    voiceVal = theItem.getString(voice);

                theItem.remove("image");
                theItem.remove("videourl");
                theItem.remove("voice");

                theItem.put("image",imageVal);
                theItem.put("videourl",videoVal);
                theItem.put("voice",voiceVal);
                System.out.println(theItem);
            }
        }

        return result;

    }

    private JSONObject getItem(JSONArray arr, String title) {
        for (int i = 0; i < arr.size(); i++) {
            JSONObject card = arr.getJSONObject(i);
            if (card.getString("title").equals(title))
                return card;
        }
        return null;
    }

    public void executeSql(String sql, List<Object> params) throws SQLException {

        HikariDataSource dataSource = null;
        Connection connection = null;
        try {
            /* 将DataSource修改为单例模式，且不close */
            dataSource = DataSource.getInstance();
            connection = dataSource.getConnection();

            PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);

            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    statement.setObject(i + 1, params.get(i));
                }
            }

            print("sql:" + sql);
            log.info("sql:" + sql);

            statement.execute();

        } catch (Exception e) {

            if (e.getMessage().contains("Duplicate entry")) {
                print("已有数据");
                log.info("已有数据");
            } else {
                e.printStackTrace();
                log.error(e.getMessage() + e.getStackTrace());
            }

        } finally {
            if (connection != null && !connection.isClosed())
                connection.close();
        }

    }

    public void print(Object obj) {
        System.out.println(obj);
    }


}
