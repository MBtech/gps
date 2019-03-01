package gps.node.master.monitoring.html;

import gps.node.MachineConfig;
import gps.node.MachineStats;
import static gps.node.master.monitoring.Constants.*;
import gps.node.master.monitoring.HttpRequestMessage;
import gps.node.master.monitoring.HttpResponseMessage;

import org.apache.log4j.Logger;

public class WorkersListPage extends HtmlPage {

	private static Logger logger = Logger.getLogger(MonitoringMainPage.class);

	public WorkersListPage(MachineConfig machineConfig, MachineStats machineStats,
		String executionName) {
		super(machineStats, machineConfig, executionName);
	}
	@Override
	public HttpResponseMessage generatePage(HttpRequestMessage request) {
		logger.info("Generating HttpResponseMessage for WorkersListPage.");
		HttpResponseMessage response = getSuccessResponseWithHtmlBodyInitialRefreshLinesAndTitle(
			request, "GPS Workers");

		response.appendBody("<h1>List of Workers</h1>");
		// Table of Workers.
		startTableTag(response);
		response.appendBody("<tr> <th>Id</th> <th>Host</th><th>Latest Status</th><th>Latest Status Receive Time</th><th>Connection Establishment Time</th></tr>\n");
		String anchorUrl;
		String hostName;
		for (int workerId : machineConfig.getWorkerIds()) {
			anchorUrl = "./stats.html?" + EXECUTION_NAME_ARG_NAME + "=" + executionName
				+ "&" + WORKER_ID_ARG_NAME + "=" + workerId;
			hostName = machineConfig.getHostPortPair(workerId).fst;
			response.appendBody("<tr><td><a href=\"" + anchorUrl +"\">" + workerId + "</a></td>"
				+ "<td>" + hostName + "</td>");
			response.appendBody("<td>" + getLatestStatusString(workerId) + "</td>");
			response.appendBody("<td>" + getLatestStatusTime(workerId) + "</td>");
			response.appendBody("<td>" + getConnectionEstablishmentTime(workerId) + "</td></tr>");
			response.appendBody("\n");
		}
		closeTableTag(response);
		closeBodyAndHtml(response);
	    logger.info("Generated HttpResponseMessage for WorkersListPage...");
		return response;
	}

	@Override
	public Logger getLogger() {
		return logger;
	}
}