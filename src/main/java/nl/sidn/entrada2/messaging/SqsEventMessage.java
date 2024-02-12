package nl.sidn.entrada2.messaging;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
public class SqsEventMessage {
    private Detail detail;
    @JsonProperty("detail-type")
    private String detailType;
    
    @Data
    @NoArgsConstructor
    public static class Detail{
    	private Bucket bucket;
    	private Object object;
    	private String reason;
    }
    
    @Data
    @NoArgsConstructor
    public static class Bucket{
    	private String name;
    }
    
    @Data
    @NoArgsConstructor
    public static class Object{
    	private String key;
    	private long size;
    }
}
