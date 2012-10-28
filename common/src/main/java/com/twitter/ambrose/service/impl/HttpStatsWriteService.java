package com.twitter.ambrose.service.impl;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.ambrose.service.DAGNode;
import com.twitter.ambrose.service.StatsWriteService;
import com.twitter.ambrose.service.WorkflowEvent;
import com.twitter.ambrose.util.JSONUtil;

public class HttpStatsWriteService implements StatsWriteService {

	private static final Logger LOG = LoggerFactory.getLogger(HttpStatsWriteService.class);

	private static final String HTTP_POST_URL = "ambrose.notification.url";

	@Override
	public void sendDagNodeNameMap(String workflowId,
			Map<String, DAGNode> dagNodeNameMap) throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void pushEvent(String workflowId, WorkflowEvent event)
			throws IOException {
		try {
			String url = System.getProperty(HTTP_POST_URL);
			if (url == null || url.length() == 0)
				LOG.error(HTTP_POST_URL + " param is empty, not notifying.");
			sendPostRequest("", event);
		} catch (Exception e) {
	        LOG.error("Could not notify workflow event", e);
		}
	}

	public static void sendPostRequest(String requestURL, WorkflowEvent event)
			throws IOException {
		URL url = new URL(requestURL);
		URLConnection urlConn = url.openConnection();
		urlConn.setUseCaches(false);
		urlConn.setDoInput(false); // true indicates the server returns response
		OutputStreamWriter writer = null;
		try {
			urlConn.setDoOutput(true); // true indicates POST request
			writer = new OutputStreamWriter(urlConn.getOutputStream());
			JSONUtil.writeJson(writer, event);
			writer.flush();
		} finally {
			if (writer != null)
				writer.close();
		}

	}
}
