package nl.sidn.entrada2.config;

import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import lombok.Data;

@Component
@ConfigurationProperties(prefix = "entrada.s3")
@Data
public class EntradaS3Properties {

    private String accessKey;
    private String secretKey;
    private String region;
    private String endpoint;
    private String bucket;
    private List<String> pcapInPrefixes;
    private String pcapDoneDir;
    private boolean pcapDelete = true;
    private String referenceDir;
    private String warehouseDir;
}
