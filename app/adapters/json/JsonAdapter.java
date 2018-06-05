package adapters.json;

import java.util.List;
import com.jayway.jsonpath.JsonPath;

public class JsonAdapter {
	public static String TEST_DATA = "{\"metadata\": {\"counterparty_historydate\": {\"case\": \"UPPPER\", \"first\": \"EDISON SPA@2014-08-01\", \"last\": \"STANDARD BANK PLC@2013-12-31\", \"size\": 3, \"sorted\": [\"EDISON SPA@2014-08-01\", \"HSBC BANK PLC@2014-01-01\", \"STANDARD BANK PLC@2013-12-31\"] }, \"transaction_id\": {\"first\": \"t100\", \"last\": \"t200\", \"size\": 3, \"sorted\": [\"t100\", \"t200\", \"t300\"] } } }";
	

	public static String find(String json, String jsonPathKey, String targetValue) {
		return JsonPath.read(json, jsonPathKey);
	}
}