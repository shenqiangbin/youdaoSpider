package youdaoSpider;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
		for (int i = 0; i < arr.size(); i++) {
			JSONObject card = arr.getJSONObject(i);
			
			BigInteger id = card.getBigInteger("id");
			String image = card.getString("image");
			String summary = card.getString("summary");
			String title = card.getString("title");
			
			StringBuilder builder = new StringBuilder();
			builder.append("insert dailyEnglish(id,image,summary,title,addTime) values (");
			builder.append(id).append(",");
			builder.append("'").append(image).append("',");
			builder.append("'").append(summary).append("',");
			builder.append("'").append(title).append("',");
			builder.append("'").append(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())).append("'");
			builder.append(")");
			
			System.out.println(builder.toString());
			
			try {
				executeSql(builder.toString());
			} catch (SQLException e) {
				log.error(e.getMessage() + e.getStackTrace());
				e.printStackTrace();
			}
		}
	}

	public void executeSql(String sql) throws SQLException {

		HikariDataSource dataSource = null;
		Connection connection = null;
		try {
			/* 将DataSource修改为单例模式，且不close */
			dataSource = DataSource.getInstance();
			connection = dataSource.getConnection();

			Statement statement = connection.createStatement();

			print("sql:" + sql);
			log.info("sql:" + sql);
			statement.execute(sql);

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
