package nl.sidn.entrada2.util;

public enum S3ObjectTagName {
	
	ENTRADA_NS_SERVER("entrada-ns-server"),
	ENTRADA_NS_ANYCAST_SITE("entrada-ns-anycast-site"),
	ENTRADA_PROCESS_TS_START("entrada-process-ts-start"),
	ENTRADA_PROCESS_TS_END("entrada-process-ts-end"),
	ENTRADA_PROCESS_DURATION("entrada-process-duration"),
	ENTRADA_PROCESSED_OK("entrada-processed-ok");

	public final String value;
	
	private S3ObjectTagName(String value) {
		this.value = value;
	}
	
}
