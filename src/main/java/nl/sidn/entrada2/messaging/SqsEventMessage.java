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



//{
//    "version": "0",
//    "id": "739cbb27-3a84-caaa-fc43-becbd2b4b047",
//    "detail-type": "Object Created",
//    "source": "aws.s3",
//    "account": "102372964922",
//    "time": "2024-02-08T07:46:20Z",
//    "region": "eu-west-1",
//    "resources": [
//        "arn:aws:s3:::sidnlabs-iceberg-data"
//    ],
//    "detail": {
//        "version": "0",
//        "bucket": {
//            "name": "sidnlabs-iceberg-data"
//        },
//        "object": {
//            "key": "pcap/trace_tokyo_2_2024-02-02_09:53:22.pcap.gz",
//            "size": 9812713,
//            "etag": "2574448a25e009d124f6a2959085e979",
//            "sequencer": "0065C486CC2E1BDFE1"
//        },
//        "request-id": "4NNTP2VVE9N6303E",
//        "requester": "102372964922",
//        "source-ip-address": "94.198.158.11",
//        "reason": "PutObject"
//    }
//}