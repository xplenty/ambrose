package com.twitter.ambrose.service.impl;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.ambrose.model.Job;
import com.twitter.ambrose.model.DAGNode;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.model.Event;
import com.twitter.ambrose.util.JSONUtil;

public class HttpStatsWriteService implements StatsWriteService<Job> {

	private static final Logger LOG = LoggerFactory.getLogger(HttpStatsWriteService.class);

	private static final String HTTP_POST_URL = "ambrose.notification.url";

	protected String url;
	
	public HttpStatsWriteService(String url) {
		this.url = url;
	}
	
	public HttpStatsWriteService() { }
	
	@Override
	public void sendDagNodeNameMap(String workflowId,
			Map<String, DAGNode<Job>> dagNodeNameMap) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void pushEvent(String workflowId, Event event)
			throws IOException {
		try {
			String url = System.getProperty(HTTP_POST_URL);
			if (url == null || url.length() == 0)
				url = this.url;
			if (url == null || url.length() == 0)
				LOG.error(HTTP_POST_URL + " param is empty, not notifying.");
			sendPostRequest(url, event);
		} catch (Exception e) {
	        LOG.error("Could not notify workflow event", e);
		}
	}

	public static void sendPostRequest(String requestURL, Event event)
			throws IOException {
		URL url = new URL(requestURL);
		URLConnection urlConn = url.openConnection();
		urlConn.setUseCaches(false);
		urlConn.setDoInput(true); // true indicates the server returns response
		OutputStreamWriter writer = null;
		try {
			urlConn.setDoOutput(true); // true indicates POST request
			urlConn.setRequestProperty("Content-Type", "application/json");
			writer = new OutputStreamWriter(urlConn.getOutputStream());
			JSONUtil.writeJson(writer, event);
			writer.flush();
			LOG.info(""+((HttpURLConnection)urlConn).getResponseCode());
		} finally {
			if (writer != null)
				writer.close();
		}

	}

	@Override
	public void initWriteService(Properties properties) throws IOException {
		// TODO Auto-generated method stub
		
	}
}
