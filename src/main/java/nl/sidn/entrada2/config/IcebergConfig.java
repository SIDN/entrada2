package nl.sidn.entrada2.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.hadoop.HadoopMetricsContext;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.types.Types.NestedField;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import nl.sidn.entrada2.load.FieldEnum;

@Configuration
public class IcebergConfig {

  @Value("${iceberg.catalog.url}")
  private String catalogUrl;

  @Value("${iceberg.warehouse_dir}")
  private String catalogWarehouse;

  @Value("${iceberg.endpoint}")
  private String catalogEndpoint;

  @Value("${iceberg.access_key}")
  private String catalogAccessKey;

  @Value("${iceberg.secret_key}")
  private String catalogSecretKey;
  
  @Value("${iceberg.catalog.token}")
  private String catalogSecurityToken;

  @Bean
  Schema schema() {
    InputStream is = getClass().getResourceAsStream("/avro/dns-query.avsc");

    try {
      org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(is);
      return validatedSchema(AvroSchemaUtil.toIceberg(avroSchema));
    } catch (IOException e) {
      throw new RuntimeException("Error creating Avro schema", e);
    }
  }


  @Bean
  RESTCatalog catalog() {
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
    properties.put(CatalogProperties.URI, catalogUrl);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, catalogWarehouse);
    properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    properties.put(OAuth2Properties.TOKEN, catalogSecurityToken);
    properties.put(S3FileIOProperties.ENDPOINT, catalogEndpoint);
    properties.put(S3FileIOProperties.SECRET_ACCESS_KEY, catalogSecretKey);
    properties.put(S3FileIOProperties.ACCESS_KEY_ID, catalogAccessKey);
    properties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");
    
    //properties.put(HadoopMetricsContext.SCHEME, "s3");
    
    RESTCatalog catalog = new RESTCatalog();
    catalog.initialize("entrada", properties);
    return catalog;

  }



  private Schema validatedSchema(Schema schema) {
    // check if schema fields match with the ordering used in FieldEnum
    // this may happen when the schema is changed but the enum is forgotten.

    for (NestedField field : schema.columns()) {

      if (field.fieldId() != FieldEnum.valueOf(field.name()).ordinal()) {
        throw new RuntimeException(
            "Ordering of Avro schema field \"" + field.name() + "\" not correct, expected: "
                + field.fieldId() + " found: " + FieldEnum.valueOf(field.name()).ordinal());
      }
    }

    return schema;
  }


}
