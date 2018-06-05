package adapters.aims;


import java.io.Serializable;

public class RecordCode implements Serializable {
	final public static RecordCode DEFAULT = new RecordCode("null", "US", 0, TimeUnit.DAY,"null","");

	public String code;
	public String name;
	public String countryCode;
	public long retention;
	public TimeUnit timeUnit;
	public String usOfficialEventType;

	private RecordCode() {}

	public RecordCode(String code, String countryCode, long retention, TimeUnit timeUnit, String name) {
		this.code = code;
		this.countryCode = countryCode;
		this.retention = retention;
		this.timeUnit = timeUnit;
		this.name=name;
	}
	
	public RecordCode(String code, String countryCode, long retention, TimeUnit timeUnit, String name, String usOfficialEventType) {
		this.code = code;
		this.countryCode = countryCode;
		this.retention = retention;
		this.timeUnit = timeUnit;
		this.name=name;
		this.usOfficialEventType=usOfficialEventType;
	}

	@Override
	public String toString() {
		return "{\"recordCode\": {\"code\": \"" + code + "\", \"retention\": \"" + retention + "\", \"timeUnit\": \"" + timeUnit + "\"}";
	}

	public long recordCodeSeconds() {
		if(retention==-1L){
			return -1;
		}else{
			return timeUnit.toSeconds(retention);
		}
	}
	public enum TimeUnit {
		YEAR(365L), 
		MONTH(365L / 12L),
		DAY(1L);

		
		private long days = 0L;
		private static long secondsPerDay = 60L * 60L * 24L;
		private static long millisecsPerDay = secondsPerDay * 1000L;

		TimeUnit(long days) {
			this.days = days;
		}

		public long retentionEnds(long maturityDate, long retention) {
			return maturityDate + toMillisecs(retention);
		}

		public long toSeconds(long retention) {
			return secondsPerDay * days * retention;
		}

		public long toMillisecs(long retention) {
			return millisecsPerDay * days * retention;
		}
	}
}