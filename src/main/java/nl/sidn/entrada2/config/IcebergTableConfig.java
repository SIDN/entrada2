package nl.sidn.entrada2.config;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@DependsOn("icebergCatalogConfig")
public class IcebergTableConfig {
  
  @Value("${iceberg.compression}")
  private String compressionAlgo;
  @Value("${iceberg.metadata.version.max:50}")
  private int metadataVersionMax;
  @Value("${iceberg.table.location}")
  private String tableLocation;
  @Value("${iceberg.table.namespace}")
  private String tableNamespace;
  @Value("${iceberg.table.name}")
  private String tableName;
//  @Value("${iceberg.table.sorted:true}")
//  private boolean enableSorting;
  
  @Autowired
  private Environment env; 
  @Autowired
  private Schema schema; 
  @Autowired
  private GlueCatalog catalog;
  

  @Bean Table table() {

    Namespace namespace = Namespace.of(tableNamespace);
    TableIdentifier tableId = TableIdentifier.of(namespace, tableName);
    
    if(env.acceptsProfiles(org.springframework.core.env.Profiles.of("controller"))) {
            
      // only create namespace and table if this is the controller
      if (!catalog.namespaceExists(namespace)) {
        log.info("Create new Iceberg namespace: {}", namespace);
        catalog.createNamespace(namespace);
      }
      
      if (!catalog.tableExists(tableId)) {
        log.info("Create new Iceberg table: {}", tableId);
        
        PartitionSpec spec = PartitionSpec.builderFor(schema)
            .day("time", "day")
            .identity("server")
            .build();
        
        Map<String, String> props = new HashMap<>();
        props.put("write.parquet.compression-codec", compressionAlgo);
        props.put("write.metadata.delete-after-commit.enabled", "true");
        props.put("write.metadata.previous-versions-max", ""+metadataVersionMax);
        props.put("format-version", "2");
      //  props.put(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "domainname","true");
        
       
        
        
       catalog.createTable(tableId, schema, spec, tableLocation, props);
        

//        if(enableSorting) {
//          t
//          .replaceSortOrder()
//          .asc("domainname")
//          .commit(); 
//        }
        
        return catalog.loadTable(tableId);
        
//        t.updateProperties().
//        	set(TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "domainname", "true").
//        commit();
        
     //   return t;
         
      }   
    }
    
    return catalog.loadTable(tableId);
  }

}
