/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package nl.sidn.icebergcatalog.rest;

import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
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
      log.error("Error starting server", e);
    }
  }

  private static Catalog backendCatalog() {
    Map<String, String> catalogProperties = new HashMap<String, String>();


    catalogProperties.put(CatalogProperties.CATALOG_IMPL, "org.apache.iceberg.jdbc.JdbcCatalog");
    catalogProperties.put(CatalogProperties.URI, "jdbc:postgresql://localhost:5432/iceberg");
    catalogProperties.put(JdbcCatalog.PROPERTY_PREFIX + "user", System.getenv().get("JDBC_USER"));
    catalogProperties.put(JdbcCatalog.PROPERTY_PREFIX + "password",
        System.getenv().get("JDBC_PASSWORD"));
    catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, "s3://testbucket1/warehouse/");
    catalogProperties.put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO");
    catalogProperties.put(S3FileIOProperties.ENDPOINT, "http://localhost:9000");
    catalogProperties.put(S3FileIOProperties.ACCESS_KEY_ID, "minio666");
    catalogProperties.put(S3FileIOProperties.SECRET_ACCESS_KEY, "minio666");
    catalogProperties.put(S3FileIOProperties.PATH_STYLE_ACCESS, "true");

    log.info("Creating catalog with properties: {}", catalogProperties);


    return CatalogUtil.buildIcebergCatalog("rest_backend", catalogProperties, null);
  }


}
