/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package nl.sidn.icebergcatalog.rest;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.apache.iceberg.rest.RESTCatalogAdapter;
import org.apache.iceberg.rest.RESTCatalogServlet;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RESTCatalogServer {

	private RESTCatalogServer() {}
  
  
	public static void main(String[] args) {
		
		log.info("Starting server");
		
		RESTCatalogAdapter adapter = new RESTCatalogAdapter(backendCatalog());
		RESTCatalogServlet servlet = new RESTCatalogServlet(adapter);
			
		Server server = new Server();
		ServerConnector connector = new ServerConnector(server);
        connector.setPort(8182);
        server.setConnectors(new Connector[] {connector});
		ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath("/");
		server.setHandler(context);
		context.addServlet(new ServletHolder(servlet), "/*");
		try {
			server.start();
		} catch (Exception e) {
			log.error("Error starting server",e);
		}
	}

	private static Catalog backendCatalog()  {
    // Translate environment variable to catalog properties
    Map<String, String> catalogProperties = new HashMap<String, String>();
//        System.getenv().entrySet().stream()
//            .filter(e -> e.getKey().startsWith(CATALOG_ENV_PREFIX))
//            .collect(
//                Collectors.toMap(
//                    e ->
//                        e.getKey()
//                            .replaceFirst(CATALOG_ENV_PREFIX, "")
//                            .replaceAll("__", "-")
//                            .replaceAll("_", ".")
//                            .toLowerCase(Locale.ROOT),
//                    Map.Entry::getValue,
//                    (m1, m2) -> {
//                      throw new IllegalArgumentException("Duplicate key: " + m1);
//                    },
//                    HashMap::new));


    catalogProperties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalog");
    catalogProperties.put(CatalogProperties.URI, "jdbc:postgresql://localhost:5432/iceberg");    
    catalogProperties.put(JdbcCatalog.PROPERTY_PREFIX + "user",  System.getenv().get("JDBC_USER"));
    catalogProperties.put(JdbcCatalog.PROPERTY_PREFIX + "password", System.getenv().get("JDBC_PASSWORD"));
    catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://testbucket1/warehouse/");
    catalogProperties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    catalogProperties.put(S3FileIOProperties.ENDPOINT, "http://localhost:9000");
    catalogProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "minio666");
    catalogProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "minio666");
    catalogProperties.put(S3FileIOProperties.PATH_STYLE_ACCESS,"true");

    log.info("Creating catalog with properties: {}", catalogProperties);
    
  //  Configuration hadoopCfg = new Configuration();
//    hadoopCfg.set("fs.s3a.endpoint","http://localhost:9000");
//    hadoopCfg.set("fs.s3a.path.style.access", "true");
//    //hadoopCfg.set("fs.s3a.impl", "org.apache.iceberg.aws.s3.S3FileIO");
//    hadoopCfg.set("fs.s3a.endpoint.region", "us-east-1");
//    hadoopCfg.set("fs.s3a.access.key", "minio666");
//    hadoopCfg.set("fs.s3a.secret.key", "minio666");
//    hadoopCfg.set("fs.s3a.path.style.access", "true");
//    hadoopCfg.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
//    hadoopCfg.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
//   

    return CatalogUtil.buildIcebergCatalog("rest_backend", catalogProperties, null);
  }

  
}
