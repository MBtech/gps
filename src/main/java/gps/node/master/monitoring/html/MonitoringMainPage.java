package gps.node.master.monitoring.html;

import org.apache.log4j.Logger;

import gps.node.MachineConfig;
import gps.node.MachineStats;
import gps.node.Pair;
import gps.node.Utils;
import gps.node.MachineStats.StatName;
import static gps.node.master.monitoring.Constants.*;
import gps.node.master.monitoring.HttpRequestMessage;
import gps.node.master.monitoring.HttpResponseMessage;

public class MonitoringMainPage extends HtmlPage {

	private static Logger logger = Logger.getLogger(MonitoringMainPage.class);

	private final String masterHostName;
	
	public MonitoringMainPage(MachineConfig machineConfig, MachineStats machineStats,
		String executionName) {
		super(machineStats, machineConfig, executionName);
		Pair<String, Integer> hostPortPair = machineConfig.getHostPortPair(Utils.MASTER_ID);
		this.masterHostName = hostPortPair != null ? hostPortPair.fst : null;
	}

	@Override
	public HttpResponseMessage generatePage(HttpRequestMessage request) {
		logger.info("Generating HttpResponseMessage for MonitoringMainPage.");
		HttpResponseMessage response =
			getSuccessResponseWithHtmlBodyInitialRefreshLinesAndTitle(request,
				"GPS Monitoring HomePage");
		response.appendBody("<h1>" + (masterHostName != null ? masterHostName : executionName)
			+ " GPS Monitoring</h1>\n");
		response.appendBody("<hr> <b>Status:</b> " + getLatestStatusString(Utils.MASTER_ID)
			+ " (Last Update: " + getLatestStatusTime(Utils.MASTER_ID) + ")<br>\n");
		response.appendBody("<b>Started:</b>" + getFormattedDate(
			machineStats.getStatValue(StatName.SYSTEM_START_TIME).longValue()) + "<br>\n");

		response.appendBody("<b>Current Job Arguments:</b>");
		for (int i = 0; ; ++i) {
			String commandLineValue = machineStats.getMachineStringStats().get(
				Utils.COMMAND_LINE_STAT_PREFIX + i);
			if (commandLineValue == null) {
				break;
			}
			String[] split = commandLineValue.split(Utils.COMMAND_LINE_STAT_VALUE_SEPARATOR);
			response.appendBody("<li> " + split[0] + ": " + split[1] + "\n");
		}
		response.appendBody("<li> JVM args:");
		for (int i = 0; ; ++i) {
			String jvmArg = machineStats.getMachineStringStats().get(Utils.JVM_ARGS_PREFIX + i);
			if (jvmArg == null) {
				break;
			}
			response.appendBody(" " + jvmArg);
		}

		response.appendBody("\n<hr>\n");
		response.appendBody("<b>Master Address:</b> "  + masterHostName + "<br>\n");
		response.appendBody("<b>Number of <a href=\"./" + WORKERS_HTML + "\">Workers:</a></b> " +
			machineConfig.getWorkerIds().size() +"<br>\n");
	    response.appendBody("<hr>\n"); 
	    appendStatistics(response);
		closeBodyAndHtml(response);
		logger.info("Generated HttpResponseMessage.");
		return response;
	}

	private void appendStatistics(HttpResponseMessage response) {
		response.appendBody("<b>Worker/Graph Statistics:</b> "
			+ "(Note: Most stats are for previous superstep. Stat Value is total or maximum across workers.)\n");
		startTableTag(response);
		response.appendBody("<tr> <th>Stat Name</th><th>Stat Value</th><th></th><th></th><th></th></tr>\n");
		String totalOrMaxRow;
		for (StatName statName : StatName.values()) {
			if (statName.isPerSuperstep()) {
				totalOrMaxRow = "<td><a href=\"./stats.html?" + EXECUTION_NAME_ARG_NAME + "=" + executionName + "&" + STAT_NAME_ARG_NAME + "=" + statName.toString().toLowerCase() + "\">total-or-max</a></td>";
			} else {
				totalOrMaxRow = "<td>total-or-max</td>";
			}
			appendStatisticToResponse(response, totalOrMaxRow, statName.toString().toLowerCase(),
				getTotalOrMaxStatValue(statName.name(), statName.isPerSuperstep(),
					statName.isTakeMaxWhenExposing()));
		}

		for (String customStatKey : machineStats.getCustomStatKeys()) {
			totalOrMaxRow = "<td><a href=\"./stats.html?" + EXECUTION_NAME_ARG_NAME + "=" + executionName + "&" + STAT_NAME_ARG_NAME + "=" + customStatKey + "\">total-or-max</a></td>";
			appendStatisticToResponse(response, totalOrMaxRow /* "<td>total-or-max</td>" */, customStatKey,
				getTotalOrMaxStatValue(customStatKey, true /* isPerSuperStep */,
					false /* take total not the max when exposing */));
		}
		closeTableTag(response);
	}

	private void appendStatisticToResponse(HttpResponseMessage response, String totalOrMaxRow,
		String statNameStr, Double statValue) {
		response.appendBody("<tr><td>" + statNameStr + "</td><td>" + statValue + "</td>"
			+ totalOrMaxRow
			+ "<td><a href=\"./stats.html?" + EXECUTION_NAME_ARG_NAME + "=" + executionName + "&" + STAT_NAME_ARG_NAME + "=" + statNameStr + "&" + LIST_ARG_NAME + "=s\">per-superstep-across-workers</td>"
			+ "<td><a href=\"./stats.html?" + EXECUTION_NAME_ARG_NAME + "=" + executionName + "&" + STAT_NAME_ARG_NAME + "=" + statNameStr + "&" + LIST_ARG_NAME + "=w\">per-worker-across-supersteps</td></tr>\n");
	}

	private Double getTotalOrMaxStatValue(String statNameString, boolean isPerSuperstep,
		boolean takeMaxWhenExposing) {
		Double retVal = null;
		if (!isPerSuperstep) {
			 retVal = machineStats.getStatValue(statNameString);
			 if (retVal == null) {
				 return machineStats.getStatValue(statNameString, null, Utils.MASTER_ID);
			 } else {
				 return retVal;
			 }
		} else {
			return getTotalOrMaxStatValue(statNameString, findLatestSuperstep(),
				takeMaxWhenExposing);
		}
	}

	@Override
	public Logger getLogger() {
		return logger;
	}
}