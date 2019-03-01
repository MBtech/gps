package gps.node.master.monitoring;

import java.util.Map;

/**
 * A HTTP request message.
 */
public class HttpRequestMessage {

	private String relativeURL = null;
	private Map<String, String> args = null;

	public HttpRequestMessage(String relativeURL, Map<String, String> args) {
		this.relativeURL = relativeURL;
		this.args = args;
	}

	public String getRelativeURL() {
		return relativeURL;
	}

	public String getParameter(String name) {
		return args.get(name);
	}
}