package gps.node.master.monitoring.html;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;

import gps.node.MachineConfig;
import gps.node.MachineStats;
import gps.node.MachineStats.StatName;
import gps.node.MachineStats.StringStatName;
import gps.node.StatusType;
import gps.node.Utils;
import gps.node.master.GPSMaster;
import static gps.node.master.monitoring.Constants.*;
import gps.node.master.monitoring.Constants;
import gps.node.master.monitoring.HttpRequestMessage;
import gps.node.master.monitoring.HttpResponseMessage;

public abstract class HtmlPage {
	
	protected final MachineStats machineStats;
	protected final MachineConfig machineConfig;
	protected final String executionName;
	
	public HtmlPage(MachineStats machineStats, MachineConfig machineConfig, String executionName) {
		this.machineStats = machineStats;
		this.machineConfig = machineConfig;
		this.executionName = executionName;
	}

	public abstract HttpResponseMessage generatePage(HttpRequestMessage request);

	public abstract Logger getLogger();
	
	protected String getFormattedDate(long timeInMillis) {
		return new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss.SSS").format(new Date(timeInMillis));
	}

	protected String getLatestStatusString(int machineId) {
		Double statValue = machineStats.getStatValue(StatName.LATEST_STATUS.name(), null, machineId);
		if (statValue == null) {
			return "no status updates has been received yet.";
		}
		StatusType status = StatusType.getTypeFromId(statValue.intValue());
		if (StatusType.STARTING_UP == status || StatusType.ESTABLISHING_TCP_CONNECTIONS == status) {
			getLogger().info("Returning status: " + status.name());
			return status.name().toLowerCase();
		} else if (StatusType.EXCEPTION_THROWN == status) {
			getLogger().info("WorkerId: " + machineId + " has thrown an exception. ");
			return status.name().toLowerCase() + "\n" + machineStats.getStringGlobalStat(
				StringStatName.EXCEPTION_STACK_TRACE, machineId);
		} else {
			getLogger().info("Returning status: " + status.name() + " superstep: "
				+ GPSMaster.currentSuperstepNo());
			return status.name().toLowerCase() + " superstepNo: " + GPSMaster.currentSuperstepNo();
		}
	}

	protected String getConnectionEstablishmentTime(int machineId) {
		return getTimeAsString(machineStats.getStatValue(
			StatName.CONNECTION_ESTABLISHMENT_TIMESTAMP.name(), null, machineId));
	}

	protected String getLatestStatusTime(int machineId) {
		return getTimeAsString(machineStats.getStatValue(StatName.LATEST_STATUS_TIMESTAMP.name(),
			null, machineId));
	}

	private String getTimeAsString(Double timestamp) {
		if (timestamp == null) {
			return null;
		} else {
			return getFormattedDate(timestamp.longValue());
		}
	}

	protected void startTableTag(HttpResponseMessage response) {
		response.appendBody("<table border=\"1\" cellpadding=\"4\">\n");
	}

	protected void closeTableTag(HttpResponseMessage response) {
		response.appendBody("</table>");
	}

	protected HttpResponseMessage getSuccessResponseWithHtmlBodyInitialRefreshLinesAndTitle(
		HttpRequestMessage request, String title) {
		HttpResponseMessage response = getSuccessResponseWithHtmlAndHeadOpeningTags();
		appendRefreshScriptAndTitleAndLines(request, title, response);
		return response;
	}

	protected void appendRefreshScriptAndTitleAndLines(HttpRequestMessage request, String title,
		HttpResponseMessage response) {
		appendRefreshScript(response);
		response.appendBody("</head>\n");
		int refreshTimeInMillis = getRefreshArgument(request);
		appendBodyWithAutoRefreshOptions(response, refreshTimeInMillis); // <body is appended here
		response.appendBody("<title>" + title + "</title>\n");
		appendRefreshLines(response, refreshTimeInMillis);
	}

	protected HttpResponseMessage getSuccessResponseWithHtmlAndHeadOpeningTags() {
		HttpResponseMessage response = new HttpResponseMessage();
		response.setContentType("text/html");
		response.setResponseCode(HttpResponseMessage.HTTP_STATUS_SUCCESS);
		response.appendBody("<html>\n");
		response.appendBody("<head>\n");
		return response;
	}

	protected void closeBodyAndHtml(HttpResponseMessage response) {
		response.appendBody("</body></html>");
	}
	
	protected void appendRefreshScript(HttpResponseMessage response) {
		response.appendBody("<!-- Codes by Quackit.com -->\n");
		response.appendBody("<script type=\"text/JavaScript\">\n");
		response.appendBody("function autoRefresh(timeoutFrequency) {\n"
			                 + "var currentUrl = window.location.href;\n"
			                 + "var urlParts = currentUrl.split(\"?\");\n"
			                 + "if (urlParts.length == 1) {\n"
			                   + "window.location += \"?refresh=\" + timeoutFrequency;\n"
			                 + "} else {\n"
			                   + "var refreshArg = null;\n"
			                   + "for (i in urlParts) {\n"
			                     + "urlPart = urlParts[i];\n"
			                     + "var args = urlPart.split(\"&\");\n"
			                     + "for (j in args) {\n"
			                       + "arg = args[j];\n"
			                       + "if (arg.indexOf(\"refresh\") != -1) {\n"
    		                         + "refreshArg = arg;\n"
			                         + "break;\n"
			                       + "}\n"
			                     +"}\n"
			                   + "}\n"
			                   + "if (refreshArg == null) {\n"
			                     + "window.location += \"&refresh=\" + timeoutFrequency;\n"
			                    + "} else {\n"
    				              + "window.location = currentUrl.replace(refreshArg, \"refresh=\" + timeoutFrequency);\n"
    				            + "}\n"
			                + "}\n"
			            + "}\n");
		response.appendBody("</script>\n");
	}

	protected Double getTotalOrMaxStatValue(String statNameString, int superstepNo,
		boolean takeMaxWhenExposing) {
		double max = 0.0;
		double total = 0.0;
		Double value;
		for (int workerId : machineConfig.getWorkerIds()) {
			value = machineStats.getStatValue(statNameString, superstepNo, workerId);
			if (value != null) {
				max = Math.max(max, value);
				total += value;
			}
		}
		if (max == 0.0) {
			return machineStats.getStatValue(statNameString, superstepNo, Utils.MASTER_ID);
		}
		return takeMaxWhenExposing ? max : total;
	}

	private void appendBodyWithAutoRefreshOptions(HttpResponseMessage response,
		int refreshTimeInMillis) {
		response.appendBody("<body onload=\"setTimeout('autoRefresh(" + refreshTimeInMillis
			+ ")', " + refreshTimeInMillis + ")\">\n");
	}

	private void appendRefreshLines(HttpResponseMessage response,
		int selectedRefreshTimeInMillis) {
		appendRefreshLine(response, DEFAULT_REFRESH_TIME,
			DEFAULT_REFRESH_TIME == selectedRefreshTimeInMillis);
		appendRefreshLine(response, LONG_REFRESH_TIME,
			LONG_REFRESH_TIME == selectedRefreshTimeInMillis);
		response.appendBody("<br>");
	}
	
	private int getRefreshArgument(HttpRequestMessage request) {
		Integer refreshTime = Constants.getIntegerArgumentOrNull(request, REFRESH_ARG_NAME,
			getLogger());
		// For any value other than the LONG_REFRESH_TIME we return DEFAULT_REFRESH_TIME.
		if (refreshTime == null || refreshTime != LONG_REFRESH_TIME) {
			return DEFAULT_REFRESH_TIME;
		} 
		return LONG_REFRESH_TIME;
	}

	private void appendRefreshLine(HttpResponseMessage response, int refreshTimeInMillis,
		boolean colorAsSelected) {
		String colorString = colorAsSelected ? "style=\"color: #800000\"": "";
		response.appendBody("<a href=\"javascript:autoRefresh(" + refreshTimeInMillis +
			")\"" + colorString + ">Refresh every " + (refreshTimeInMillis / 1000) + " seconds"
			+ "</a>\n");
	}

	protected int findLatestSuperstep() {
		int latestSuperstepPlusOne = 1;
		while (machineStats.getStatValue(StatName.NUM_VERTICES.name(), latestSuperstepPlusOne , 0)
			!= null) {
			latestSuperstepPlusOne++;
		}
		return latestSuperstepPlusOne - 1;
	}
}