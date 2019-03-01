package gps.node.master.monitoring.html;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import gps.node.MachineConfig;
import gps.node.MachineStats;
import gps.node.MachineStats.StatName;
import static gps.node.master.monitoring.Constants.*;
import gps.node.master.monitoring.Constants;
import gps.node.master.monitoring.HttpRequestMessage;
import gps.node.master.monitoring.HttpResponseMessage;

import org.apache.log4j.Logger;

public class StatsPage extends HtmlPage {

	private static Logger logger = Logger.getLogger(StatsPage.class);

	public StatsPage(MachineStats machineStats, MachineConfig machineConfig, String executionName) {
		super(machineStats, machineConfig, executionName);
	}

	@Override
	public HttpResponseMessage generatePage(HttpRequestMessage request) {
		Integer workerId = Constants.getIntegerArgumentOrNull(request, WORKER_ID_ARG_NAME, logger);
		Integer superstepNo = Constants.getIntegerArgumentOrNull(request, SUPERSTEP_NO_ARG_NAME,
			logger);
		String strStatName = request.getParameter(STAT_NAME_ARG_NAME);
		if (!machineStats.isCustomStatKey(strStatName)) {
			strStatName = strStatName.toUpperCase();
		}
		boolean isPerSuperstepStat = machineStats.isPerSuperstepStat(strStatName);
		String listType = request.getParameter(LIST_ARG_NAME);
		logger.info("Generating HttpResponseMessage for StatsPage. workerId: " + workerId
			+ " superstepNo: " + superstepNo + " statName: " + strStatName
			+ " isPerSuperstepStat: " + isPerSuperstepStat);
		HttpResponseMessage response = null;
		if (workerId != null) {
			if (superstepNo != null) {
				response = generateWorkerStatsPageForParticularSuperstep(request, workerId,
					superstepNo);
			} else if (strStatName != null) {
				response = generateStatsPageForParticularWorkerAcrossSupersteps(request, strStatName,
					workerId);
			} else {
				response = generateWorkerStatsPageForParticularSuperstep(request, workerId,
					findLatestSuperstep());
			}
		} else if (superstepNo != null) {
			if (strStatName != null) {
				response = generateStatsPageForParticularSuperstepAcrossWorkers(request, strStatName,
					superstepNo);
			}
		} else if ("s".equalsIgnoreCase(listType)) {
			List<Integer> listOfSupersteps = new ArrayList<Integer>();
			for (int i = 1; i <= findLatestSuperstep(); ++i) {
				listOfSupersteps.add(i);
			}
			response = generateList(request, listOfSupersteps, strStatName, true /* is superstep list */);
		} else if ("w".equalsIgnoreCase(listType)) {
			response = generateList(request, new ArrayList<Integer>(machineConfig.getWorkerIds()),
				strStatName, false /* is workers list */);
		} else if (strStatName != null && isPerSuperstepStat) {
			response = generateStatsPageForTotalOrMaxAcrossSupersteps(request, strStatName);
		} else {
			response = generateRedirectToMonitoringHtml();
		}
		logger.info("Generated HttpResponseMessage for StatsPage.");
		return response;
	}

	private HttpResponseMessage generateList(HttpRequestMessage request, List<Integer> listItems,
		String strStatName, boolean isSuperstepList) {
		String headerAndTitle =  isSuperstepList ? "Supersteps" : "Workers";
		HttpResponseMessage response = getSuccessResponseWithHtmlBodyInitialRefreshLinesAndTitle(
			request, headerAndTitle);
		response.appendBody("<b>" + headerAndTitle + ":</b><br>");
		Collections.sort(listItems);
		String secondArg = isSuperstepList ? SUPERSTEP_NO_ARG_NAME : WORKER_ID_ARG_NAME;
		for (int listItem : listItems) {
			response.appendBody("<a href=\"./stats.html?" + EXECUTION_NAME_ARG_NAME + "="
				+ executionName + "&" + STAT_NAME_ARG_NAME + "="
				+ strStatName.toString().toLowerCase() + "&" + secondArg + "=" + listItem + "\">"
				+ listItem + "<a><br>");
		}
		closeBodyAndHtml(response);
		return response;
	}

	private StatName getStatName(HttpRequestMessage request) {
		String strStatName = request.getParameter(STAT_NAME_ARG_NAME);
		if (strStatName == null) {
			return null;
		}
		try {
			return StatName.valueOf(strStatName.toUpperCase());
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
			logger.error("Given statName argument is invalid. Ignoring it. argName: "
				+ strStatName);
			return null;
		}
	}

	private HttpResponseMessage generateRedirectToMonitoringHtml() {
		HttpResponseMessage response = getSuccessResponseWithHtmlAndHeadOpeningTags();
		response.appendBody("<meta HTTP-EQUIV=\"REFRESH\" content=\"0; url=./" + MONITORING_HTML
			+ "\">\n");
		response.appendBody("</head>\n");
		closeBodyAndHtml(response);
		return response;
	}

	private HttpResponseMessage generateStatsPageForParticularSuperstepAcrossWorkers(
		HttpRequestMessage request, String strStatName, int superstepNo) {
		Map<Integer, Double> valuesPerWorker = new HashMap<Integer, Double>();
		for (int workerId : machineConfig.getWorkerIds()) {
			addStatValueToMap(strStatName, superstepNo, valuesPerWorker,
				machineStats.getStatValue(strStatName,
					superstepNo, workerId), workerId /* key */);
		}
		return getChartResponse(request, valuesPerWorker, "Stats Across Workers",
			strStatName + " For Superstep " + superstepNo + " Across Workers", strStatName,
			"workers" /* x axis title */, getYAxisTitle(strStatName), false /* is bar chart */);
	}

	private String getYAxisTitle(String strStatName) {
		boolean timeStatistics = strStatName.toLowerCase().contains("time");
		String yAxisTitle = strStatName.toLowerCase();
		if (timeStatistics) {
			yAxisTitle += " (in secs)";
		}
		return yAxisTitle;
	}
	
	private void addStatValueToMap(String strStatName, int superstepNo,
		Map<Integer, Double> valuesMap, Double statValue, int key) {
		if (statValue == null) {
			logger.error("No stat value for statName: "
				+ strStatName + " superstepNo: " + superstepNo + " for key: " + key);
			return;
		}
		boolean timeStatistics = strStatName.toLowerCase().contains("time");
		if (timeStatistics) {
			valuesMap.put(key, statValue / 1000 /* (converting in to seconds) */);
		} else {
			valuesMap.put(key, statValue);
		}	
	}

	private HttpResponseMessage generateStatsPageForTotalOrMaxAcrossSupersteps(
		HttpRequestMessage request, String strStatName) {
		Map<Integer, Double> valuesPerSuperstep = new HashMap<Integer, Double>();
		int latestSuperstep = findLatestSuperstep();
		for (int superstepNo = 1; superstepNo < latestSuperstep + 1; ++superstepNo) {
			addStatValueToMap(strStatName, superstepNo, valuesPerSuperstep,
				getTotalOrMaxStatValue(strStatName, superstepNo,
					machineStats.isTakeMaxWhenExposingStat(strStatName)), superstepNo);
		}
		return getChartResponse(request, valuesPerSuperstep, "Stats Across Supersteps",
			"Total/Max " + strStatName + " Across All Workers"/* title */, strStatName,
			"supersteps" /* x axis label */,  getYAxisTitle(strStatName), true /* is line chart */);
	}

	private HttpResponseMessage generateStatsPageForParticularWorkerAcrossSupersteps(
		HttpRequestMessage request, String strStatName, int workerId) {
		Map<Integer, Double> valuesPerSuperstep = new HashMap<Integer, Double>();
		int latestSuperstep = findLatestSuperstep();
		for (int superstepNo = 1; superstepNo < latestSuperstep + 1; ++superstepNo) {
			addStatValueToMap(strStatName, superstepNo, valuesPerSuperstep,
				machineStats.getStatValue(strStatName, superstepNo, workerId),
				superstepNo /* key */);
		}
		return getChartResponse(request, valuesPerSuperstep, "Stats Across Supersteps",
			strStatName + " For Worker " + workerId + " Across Supersteps" /* title */, strStatName,
			"supersteps" /* x axis label */,  getYAxisTitle(strStatName),true /* is line chart */);
	}

	private HttpResponseMessage getChartResponse(HttpRequestMessage request,
		Map<Integer, Double> data, String title, String header, String strStatName,
		String xAxisLabel, String yAxisLabel, boolean isLineChart) {
		HttpResponseMessage response = getSuccessResponseWithHtmlAndHeadOpeningTags();
		response.appendBody(
			"<script type=\"text/javascript\" src=\"https://www.google.com/jsapi\"></script>");
		response.appendBody("<script type=\"text/javascript\">");
		response.appendBody(
			"google.load(\"visualization\", \"1\", {packages:[\"corechart\"]});\n"
	      + "google.setOnLoadCallback(drawChart);\n"
	      + "function drawChart() {\n"
	      + "  var data = new google.visualization.DataTable();\n"
	      + "  data.addColumn('string'," + " '" + xAxisLabel + "');\n"
	      + "  data.addColumn('number'," + " '" + strStatName + "');\n"
	      + "  data.addRows(" +  data.size() + ");\n");
		List<Integer> keys = new ArrayList<Integer>(data.keySet());
		Collections.sort(keys);
		List<String> valuesList = new ArrayList<String>();
		for (int i = 0; i < keys.size(); ++i) {
			response.appendBody("data.setValue(" + i + ", 0, '" + keys.get(i) + "');\n");
			Double dataValue = data.get(keys.get(i));
			String dataValueStr = dataValue == null ? "0.0" : dataValue + "";
			response.appendBody("data.setValue(" + i + ", 1, " + dataValueStr + ");\n");
			valuesList.add(dataValueStr);
		}
		String chartTypeStr = isLineChart ? "LineChart" : "ColumnChart";
		response.appendBody("var chart = new google.visualization." + chartTypeStr
			+ "(document.getElementById('chart_div'));\n");
		int width = (int) (Math.ceil((double) data.size() / 10) + 1) * 300;
		response.appendBody("chart.draw(data, {width: " + width + ", height: 480, title: '"
			+ header + "',"
			+ " hAxis: {title: '" + xAxisLabel + "'},"
			+ " vAxis: {title: '" + yAxisLabel + "'}"
			+ "});\n");
		response.appendBody("}\n");
		response.appendBody("</script>");
		appendRefreshScriptAndTitleAndLines(request, title, response);
		response.appendBody("<h3>" + header + "</h3>\n");
		response.appendBody("<div id=\"chart_div\"></div>\n");
		if (request.getParameter(VERBOSE_ARG_NAME) != null) {
			for (String value : valuesList) {
				response.appendBody(value + "<br />");
			}
		}
		closeBodyAndHtml(response);
		return response;
	}

	private HttpResponseMessage generateWorkerStatsPageForParticularSuperstep(
		HttpRequestMessage request, int workerId, int superstepNo) {
		int latestSuperstep = findLatestSuperstep();
		if (superstepNo > latestSuperstep || superstepNo < 0) {
			logger.info("Given superstepNo value is either larger than the current superstepNo or "
				+ "negative. superstepNo: " + superstepNo);
			return generateRedirectToMonitoringHtml();
		}
		HttpResponseMessage response = getSuccessResponseWithHtmlBodyInitialRefreshLinesAndTitle(
			request, "Worker Statistics");
		response.appendBody("<h1>Worker " +  workerId + " Statistics For Superstep: " + superstepNo
			+ "</h1>\n");
		startTableTag(response);
		response.appendBody("<tr><th>Stat Name</th><th>Stat Value</th></tr>");
		Double statValue = null;
		String statValueStr = null;
		for (StatName statName : StatName.values()) {
			if (!statName.isPerSuperstep()) {
				continue;
			}
			statValue = machineStats.getStatValue(statName.name(), superstepNo, workerId);
			statValueStr = statValue == null ? null : "" + statValue;
			response.appendBody("<tr><td>" + "<a href=\"./stats.html?"
				+ EXECUTION_NAME_ARG_NAME + "=" + executionName
				+ "&" + WORKER_ID_ARG_NAME + "=" + workerId
				+ "&" + STAT_NAME_ARG_NAME + "=" + statName.name().toLowerCase() + "\">"
				+ statName.toString().toLowerCase() + "</a></td><td>" + statValueStr
				+ "</td></tr>\n");
		}
		closeTableTag(response);
		closeBodyAndHtml(response);
		return response;
	}

	@Override
	public Logger getLogger() {
		return logger;
	}
}