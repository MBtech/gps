package gps.node.master.monitoring.debug;

import java.util.Set;

import org.apache.log4j.Logger;

import static gps.node.master.monitoring.Constants.*;
import gps.node.master.monitoring.HttpRequestMessage;
import gps.node.master.monitoring.HttpResponseMessage;
import gps.node.master.monitoring.html.HtmlPage;

public class DebugMonitoringMainPage extends HtmlPage {

	private static Logger logger = Logger.getLogger(DebugMonitoringMainPage.class);
	private final Set<String> executionNames;

	public DebugMonitoringMainPage(Set<String> executionNames) {
		super(null, null, "");
		this.executionNames = executionNames;
	}

	@Override
	public HttpResponseMessage generatePage(HttpRequestMessage request) {
		logger.info("Generating HttpResponseMessage for DebugMonitoringMainPage.");
		HttpResponseMessage response =
			getSuccessResponseWithHtmlAndHeadOpeningTags();
		response.appendBody("</head>\n");
		response.appendBody("<title>" + "GPS Debug Monitoring HomePage" + "</title>\n");
		response.appendBody("<h1> GPS Debug Monitoring</h1>\n");
		response.appendBody("<b>Execution Names:</b> \n");
		startTableTag(response);
		response.appendBody("<tr><th>Execution Name</th><th>Main Page</th></tr>\n");
		for (String execName : executionNames) {
			response.appendBody("<tr><td>" + execName + "</td>"
				+ "<td><a href=\"./monitoring.html?" + EXECUTION_NAME_ARG_NAME + "=" + execName +
				"&" + LIST_ARG_NAME + "=s\">main page</td></tr>\n");
		}
		closeBodyAndHtml(response);
		logger.info("Generated HttpResponseMessage.");
		return response;
	}

	@Override
	public Logger getLogger() {
		return logger;
	}
}