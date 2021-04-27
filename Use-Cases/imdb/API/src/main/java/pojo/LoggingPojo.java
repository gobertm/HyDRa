package pojo;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class LoggingPojo implements Serializable, Cloneable, IPojo {
	protected List<String> logEvents = new ArrayList<String>();

	public List<String> getLogEvents() {
		return this.logEvents;
	}

	public void setLogEvents(List<String> logEvents) {
		this.logEvents = logEvents;
	}

	public void addLogEvent(String logEvent) {
		this.logEvents.add(logEvent);
	}
}
