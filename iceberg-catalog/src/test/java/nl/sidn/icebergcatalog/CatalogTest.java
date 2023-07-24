package nl.sidn.icebergcatalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class CatalogTest {
	
	public RESTCatalog catalog() {
		Map<String, String> properties = new HashMap<>();
		properties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.rest.RESTCatalog");
		properties.put(CatalogProperties.URI, "http://localhost:8182");
		//properties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3a://testbucket1/warehouse");
		properties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
		properties.put(S3FileIOProperties.ENDPOINT, "http://localhost:9000");

		RESTCatalog catalog = new RESTCatalog();
		Configuration conf = new Configuration();
		catalog.setConf(conf);
		catalog.initialize("demo", properties);

		return catalog;
		
	}

	@Test
	public void testCreateTable() {

		RESTCatalog catalog = catalog();
		
		Schema schema = new Schema(Types.NestedField.required(1, "level", Types.StringType.get()),
				Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
				Types.NestedField.required(3, "message", Types.StringType.get()),
				Types.NestedField.optional(4, "call_stack", Types.ListType.ofRequired(5, Types.StringType.get())));
		
		PartitionSpec spec = PartitionSpec.builderFor(schema)
			      .hour("event_time")
			      .build();
		

		Namespace namespace = Namespace.of("entrada");
	//	catalog.createNamespace(namespace);
		
		
		List<Namespace> nsl = catalog.listNamespaces();
		System.out.println(nsl);
		
		TableIdentifier name = TableIdentifier.of(namespace, "dns");	
		catalog.createTable(name, schema);
		//catalog.createTable(name, schema, spec);
		
		List<TableIdentifier> tables = catalog.listTables(namespace);
		System.out.println(tables);
		
	}
	
	
	@Test
	public void testDropTable() {

		RESTCatalog catalog = catalog();
		

		Namespace namespace = Namespace.of("webapp");

		
		TableIdentifier name = TableIdentifier.of(namespace, "logs");	
		catalog.dropTable(name);

		
	}
	
	@Test
	public void testInsertIntoTable() throws Exception{

		RESTCatalog catalog = catalog();
		
		Schema schema = new Schema(
			      Types.NestedField.optional(1, "event_id", Types.StringType.get()),
			      Types.NestedField.optional(2, "username", Types.StringType.get()),
			      Types.NestedField.optional(3, "userid", Types.IntegerType.get()),
			      Types.NestedField.optional(4, "api_version", Types.StringType.get()),
			      Types.NestedField.optional(5, "command", Types.StringType.get())
			    );

			Namespace webapp = Namespace.of("webapp");
			TableIdentifier name = TableIdentifier.of(webapp, "user_events");
			Table table = catalog.createTable(name, schema, PartitionSpec.unpartitioned());
			
			
			GenericRecord record = GenericRecord.create(schema);
			ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();
			builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Bruce", "userid", 1, "api_version", "1.0", "command", "grapple")));
			builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Wayne", "userid", 1, "api_version", "1.0", "command", "glide")));
			builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Clark", "userid", 1, "api_version", "2.0", "command", "fly")));
			builder.add(record.copy(ImmutableMap.of("event_id", UUID.randomUUID().toString(), "username", "Kent", "userid", 1, "api_version", "1.0", "command", "land")));
			ImmutableList<GenericRecord> records = builder.build();
			
			
			String filepath = table.location() + "/" + UUID.randomUUID().toString();
			OutputFile file = table.io().newOutputFile(filepath);
			DataWriter<GenericRecord> dataWriter =
			    Parquet.writeData(file)
			        .schema(schema)
			        .createWriterFunc(GenericParquetWriter::buildWriter)
			        .overwrite()
			        .withSpec(PartitionSpec.unpartitioned())
			        .build();

			try {
			    for (GenericRecord r : builder.build()) {
			        dataWriter.write(r);
			    }
			} finally {
			    dataWriter.close();
			}
			
			DataFile dataFile = dataWriter.toDataFile();
			

			//Namespace webapp = Namespace.of("webapp");
			//TableIdentifier name = TableIdentifier.of(webapp, "user_events");
			//Table tbl = catalog.loadTable(name);
			table.newAppend().appendFile(dataFile).commit();
			
			
		
	}

}
