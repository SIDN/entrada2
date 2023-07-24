package nl.sidn.entradacontroller;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HiveTest {
	
	@Test
	public void testMetastore() {
		log.info("Test metastore");
		
//		HiveCatalog catalog = new HiveCatalog();
//		catalog.setConf(spark.sparkContext().hadoopConfiguration());  // Configure using Spark's Hadoop configuration
//
//		Map <String, String> properties = new HashMap<String, String>();
//		properties.put("warehouse", "...");
//		properties.put("uri", "...");
//
//		catalog.initialize("hive", properties);
	}

}
