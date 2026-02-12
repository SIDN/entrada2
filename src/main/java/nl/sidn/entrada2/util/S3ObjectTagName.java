package nl.sidn.entrada2.util;

public enum S3ObjectTagName {
	
	ENTRADA_NS_SERVER("entrada-ns-server"),
	ENTRADA_NS_ANYCAST_SITE("entrada-ns-anycast-site"),
	ENTRADA_PROCESS_TS_START("entrada-process-ts-start"),
	ENTRADA_PROCESS_TS_END("entrada-process-ts-end"),
	ENTRADA_PROCESS_DURATION("entrada-process-duration"),
	ENTRADA_WAIT_EXPIRED("entrada-wait-expired"),
	ENTRADA_OBJECT_TRIES("entrada-object-tries"),
	// (ISO_8601)
	ENTRADA_OBJECT_DETECTED("entrada-object-detected"),
	// example: 2022-10-12T01:01:00.000Z
	ENTRADA_OBJECT_TS("entrada-object-ts");

	public final String value;
	
	private S3ObjectTagName(String value) {
		this.value = value;
	}
	
}
