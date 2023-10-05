package nl.sidn.entrada2.config;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;

@Configuration
@DependsOn("icebergCatalogConfig")
public class IcebergTableConfig {
  
  @Value("${iceberg.compression}")
  private String compressionAlgo;
  
  @Autowired
  private Environment env; 
  @Autowired
  private Schema schema; 
  @Autowired
  private RESTCatalog catalog;
  

  @Bean Table table() {

    Namespace namespace = Namespace.of("entrada");
    TableIdentifier tableId = TableIdentifier.of(namespace, "dns");
    
    if(env.acceptsProfiles(org.springframework.core.env.Profiles.of("controller"))) {
            
      // only create namespace and table if this is the controller
      if (!catalog.namespaceExists(namespace)) {
        catalog.createNamespace(namespace);
      }
      
      if (!catalog.tableExists(tableId)) {
        
        PartitionSpec spec = PartitionSpec.builderFor(schema)
            .day("time", "day")
            .identity("server")
            .build();
        
        Table table = catalog.createTable(tableId, schema, spec);
        table
            .updateProperties()
            .set("write.parquet.compression-codec", compressionAlgo)
            .set("write.metadata.delete-after-commit.enabled", "true")
            .set("write.metadata.previous-versions-max", "50")
            .commit();

        // Sort disabled as writer does not sort
        // enable again when writer supports sort
        // this.table.replaceSortOrder()
        // .asc("domainname")
        // .commit();
      }   
    }
    
    return  catalog.loadTable(tableId);
  }

}
