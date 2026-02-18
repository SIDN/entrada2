# ENTRADA2

ENTRADA2 is a scalable, high-performance platform for processing, enriching, and analyzing DNS traffic data. It ingests raw DNS packet captures (PCAP), transforms them into efficient [Apache Parquet](https://parquet.apache.org/) files, and stores them in the [Apache Iceberg](https://iceberg.apache.org/) open table format for advanced analytics. ENTRADA2 enriches DNS records with geolocation, ASN, public resolver detection, and other metadata, supporting real-time and batch workflows across local, Kubernetes, and AWS environments. With built-in support for S3-compatible storage, message queuing, and integration with query engines such as Spark, Trino and Athena, ENTRADA2 enables powerful, flexible analysis of DNS data for security, research, and operational insights.

ENTRADA2 is an improvement on [ENTRADA](https://github.com/SIDN/entrada) and includes support for multiple deployment types: Kubernetes, AWS and local deployment using Docker Compose.

A DNS query and response are combined into a single record and enriched by adding the following information:

- Geolocation (Country)
- Autonomous system (ASN)
- Domain name public suffix
- Detection of public resolvers (Google, OpenDNS, Quad9 and Cloudflare)
- TCP round-trip time (RTT)

## Features

ENTRADA2 supports the following features: 

- S3 storage (Rustfs, AWS)
- Open table format (Apache Iceberg)
- Message queuing (RabbitMQ, AWS SQS)
- Metadata catalog (AWS Glue or Iceberg JDBC/REST catalog)
- Metrics ([InfluxDB](https://www.influxdata.com/))
- Query engine (Trino, AWS Athena, Spark)

## Changes from ENTRADA

ENTRADA2 includes the following improvements and changes compared to ENTRADA:

- Removed Hadoop support
- Improved support for S3 object storage
- Automatically create required resources (S3 bucket and messaging queues)
- Added support for Kubernetes
- Added support for Apache Iceberg table format
- Split the QNAME into dns_domainname and dns_qname (labels before domain name) columns
- Rows are sorted by domainname for more efficient compression
- Default column compression changed to ZSTD (was Snappy)
- Bloomfilter for domainname column for performance boost when filtering on domainname
- Use small Parquet max dictionary size to prevent domainname column using dictionary instead of Bloomfilter
- Renamed table columns to indicate protocol source
- Removed unused columns
- Name server site pcaps can be processed by any container (no longer dedicated per server)
- No longer saving state between pcaps (might cause some unmatched packets)
- Added S3 event-based workflow (S3 new object scanning also supported)
- Added API to control containers (e.g., status/start/stop)
- Added option to include answer/authoritative/additional sections of DNS response in Parquet output
- Added support for decoding [Extended RCODE](https://datatracker.ietf.org/doc/html/rfc6891#section-6.1.3)
- Added support for [Extended DNS Errors](https://datatracker.ietf.org/doc/html/rfc8914)

## Deployment Modes

The following deployment modes are supported:

- Local/Single system using Docker Compose (best for test and evaluation)
- Kubernetes
- AWS

## Build

```bash
export TOOL_VERSION=0.0.10
mvn clean && mvn package && docker build --tag=sidnlabs/entrada2:$TOOL_VERSION .
```

## GeoIP Data

ENTRADA2 uses the [Maxmind](https://www.maxmind.com) GeoIP2 database. If you have a license, set the `MAXMIND_LICENSE_PAID` environment variable. Otherwise, sign up for the free [GeoLite2](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data) database and use the `MAXMIND_LICENSE_FREE` environment variable.

## Quick Start

To get started quickly, use the provided [Docker Compose script](https://github.com/SIDN/entrada2/blob/main/docker/docker-compose.yml) to create a test environment on a single host, containing all required services.

The configuration settings for the Docker Compose script can be found as environment variables in `docker.env`. The example uses the default Docker Compose script and starts 1 ENTRADA master container and 2 ENTRADA worker containers. The script has support for profiles - the `test` profile will also start ENTRADA containers, any other profile will only start the dependencies.

```bash
export MAXMIND_LICENSE_PAID=<your-key-here>
docker compose --profile test up --scale entrada-worker=2
```

The Docker Compose script uses the "test" profile to start the ENTRADA containers. Not using this profile will only start the dependencies and not the ENTRADA containers.

## Processing PCAP Data

ENTRADA2 converts a single PCAP file into one or more Parquet files. Many small input PCAP files may be automatically combined into a larger Parquet file. The `iceberg.parquet.min-records` option determines how many records a Parquet output file must contain before it is allowed to be closed and added to the Iceberg table.

The PCAP processing workflow uses 3 prefixes for PCAP objects in S3, defaults are:

- `pcap-in-prefixes`: New PCAP objects must be uploaded using a prefix from this list (e.g., "pcap-in/provider1") to trigger processing of the PCAP file
- `pcap-done`: When the auto-delete option is not enabled, processed PCAP objects are moved to this prefix
- `pcap-failed`: When a PCAP object cannot be processed correctly, it is moved to this prefix

When all components have started up, you may upload a PCAP file to the S3 bucket using a prefix from the "pcap-in-prefixes" list. If ENTRADA2 created an S3 bucket notification configuration (enabled by default), then processing of the PCAP file will automatically start.

If the S3 storage solution used does not support events for newly uploaded objects, then enable scanning for new objects. Enable S3 object scanning by setting a value for the `entrada.schedule.new-object-min` property. The scanning feature will scan the S3 bucket every x minutes for new objects and will process the objects similar to how they would be processed when an event was created for the object.

Use the following S3 tags when uploading files to S3:

- `entrada-ns-server`: Logical name of the name server (e.g., ns1.dns.nl)
- `entrada-ns-anycast-site`: Anycast site of the name server (e.g., ams)
- `entrada-object-ts`: (optional) ISO 8601 timestamp when PCAP was created (e.g., 2022-10-12T01:01:00.000Z)

Timestamps used in PCAP files are assumed to be using timezone UTC. The timestamp in the `entrada-object-ts` tag is used for sorting new S3 objects when multiple new objects are detected. This allows for bulk uploading older data and maintaining the correct order when processing. The tag `entrada-object-ts` is only used when rustfs/AWS S3 events are not configured and ENTRADA2 must periodically scan for newly uploaded objects itself.

### Upload Example

```bash
aws s3api put-object \
    --bucket entrada-bucket \
    --key pcap-in/provider1/trace_tokyo_2_2023-08-14_09_20_26.pcap.gz \
    --body trace_tokyo_2_2023-08-14_09_20_26.pcap.gz \
    --tagging "entrada-object-ts=2024-10-12T01%3A01%3A00.000Z&entrada-ns-anycast-site=Amsterdam&entrada-ns-server=ns1.dns.nl"
```

## Flushing Output

To prevent many small Parquet output files when the input stream contains many small PCAP files, there is a configuration option (`iceberg.parquet.min-records`, default 1000000) for setting the lower limit of the number of records in a Parquet output file. Output files are not closed and added to the table until this limit is reached. To force closing the output file(s), e.g., when doing maintenance, use the "flush" API endpoint.

`PUT https://hostname:8080/api/v1/state/flush` will cause all current open files to be closed and added to the table.

## Analyzing Results

The results may be analyzed using different tools, such as AWS Athena, Trino or Apache Spark. The Docker Compose script automatically starts Trino for quickly analyzing a limited dataset.

### Using Trino CLI

Example using the Trino command-line client (installed in Trino container):

```bash
docker exec -it docker-trino-1 trino
```

Switch to the correct catalog and namespace:

```sql
use iceberg.entrada2;
```

### Query Examples

Count rows in the DNS table:

```sql
select count(1)
from dns;
```

Show partitions of DNS table:

```sql
select * 
from "dns$partitions"
```

Get the domain names that received the most queries (top 25):

```sql
select dns_domainname, count(1)
from dns
group by 1
order by 2 desc
limit 25;
```

Get the most common RCODEs per day:

```sql
select day(time), dns_rcode, count(1)
from dns
group by 1,2
order by 1 asc
limit 25;
```

Get the top 10 domains on a specific date:

```sql
select dns_domainname, count(1)
from dns
where date_trunc('day', time) = timestamp '2024-07-01'
group by 1
order by 2 desc
limit 10;
```

### Working with Response Data

When response data is enabled (`entrada.rdata.enabled`), records in the rdata column can be analyzed using lambda expressions.

Get all queries for a specific rdata response value:

```sql
select dns_qname, dns_domainname, dns_rdata 
from dns 
where any_match(dns_rdata, r -> r.data = '185.159.199.200') and dns_domainname = 'dns.nl'
limit 100;
```

Get top 25 by A-records in responses:

```sql
select d.data, count(1)
from dns, UNNEST(dns.dns_rdata) d
where d.type = 1 
group by 1 
order by 2 desc  
limit 25;
```

## Cleanup

To cleanup the test environment, stop the Docker containers, delete the Docker volumes and restart the containers:

```bash
docker compose down
docker system prune -f
docker volume rm docker_entradaInfluxDbVolume docker_entradaRustfsVolume docker_entradaPostgresqlVolume;
docker compose --profile test up --scale entrada-worker=2
```

## Configuration

The ENTRADA2 configuration options are in the [Spring Boot configuration file](https://raw.githubusercontent.com/SIDN/entrada2/main/src/main/resources/application.yml). All options in this file can be overridden by using environment variables, or in the case of Kubernetes you can also create a ConfigMap containing a custom configuration file.

### ENTRADA Configuration Options

The following table lists all configuration options starting with `entrada.`:

| Option | Description | Default |
| ------ | ----------- | ------- |
| `entrada.tlds` | Comma-separated list of most frequently used TLDs for fast-path optimization | `nl` |
| `entrada.nameserver.default-name` | Default name server name when S3 objects have no tags | `default-ns` |
| `entrada.nameserver.default-site` | Default anycast site when S3 objects have no tags | `default-site` |
| `entrada.rdata.enabled` | Enable rdata from DNS response records in Parquet output (dns_rdata column) | `false` |
| `entrada.rdata.dnssec` | Include DNSSEC RRs such as RRSIG in output | `false` |
| `entrada.cname.enabled` | Enable CNAME record processing | `true` |
| `entrada.filter.tlds` | TLDs to filter out - DNS data for these TLDs will not be stored | (empty) |
| `entrada.object.max-wait-time-secs` | Max wait time before marking object as not picked up and resending to queue | `3600` |
| `entrada.object.max-proc-time-secs` | Max processing time before marking object as failed | `7200` |
| `entrada.object.max-tries` | Max number of attempts for decoding an object | `2` |
| `entrada.process.max-proc-time-secs` | Max time for PCAP processing before worker is marked as stalled | `600` |
| `entrada.process.max-request-cache-size` | Max size of internal DNS request cache for matching requests to responses | `1000000` |
| `entrada.schedule.updater-min` | Interval in minutes to update reference data | `120` |
| `entrada.schedule.liveness-min` | Interval in minutes to check for stalled workers | `1` |
| `entrada.schedule.expired-object-min` | Interval in minutes to check for expired objects | `10` |
| `entrada.schedule.new-object-secs` | Interval in seconds to check for new objects (disabled when empty) | (empty) |
| `entrada.security.token` | Token for REST API and actuator endpoints access (use X-API-KEY HTTP header) | `94591089610224297274859827590711` |
| `entrada.metrics.bin-size-secs` | Time bin size in seconds for historical metrics aggregation | `10` |
| `entrada.privacy.enabled` | When enabled, IP addresses are not written to Parquet files | `false` |
| `entrada.s3.access-key` | S3 access key for bucket containing source PCAPs | (empty) |
| `entrada.s3.secret-key` | S3 secret key for bucket containing source PCAPs | (empty) |
| `entrada.s3.region` | S3 region | `${AWS_REGION}` |
| `entrada.s3.endpoint` | S3 endpoint (only when not using AWS) | (empty) |
| `entrada.s3.bucket` | S3 bucket name for PCAP files and Iceberg data | `sidnlabs-iceberg-data` |
| `entrada.s3.pcap-in-prefixes` | List of S3 prefixes for new PCAP files | `pcap-in/provider1`, `pcap-in/provider2` |
| `entrada.s3.pcap-done-dir` | Directory for processed PCAP files (deleted if empty) | (empty) |
| `entrada.s3.pcap-delete` | Delete PCAPs after processing | `true` |
| `entrada.s3.reference-dir` | Directory for reference data | `reference` |
| `entrada.s3.warehouse-dir` | Directory for Iceberg data files | `database` |
| `entrada.messaging.request.name` | Queue name for S3 bucket lifecycle events | `entrada-s3-event` |
| `entrada.messaging.request.ttl` | Queue TTL in minutes | `60` |
| `entrada.messaging.request.aws.retention` | AWS SQS message retention period in seconds | `86400` |
| `entrada.messaging.request.aws.visibility-timeout` | AWS SQS visibility timeout in seconds | `600` |
| `entrada.messaging.command.name` | Queue name for sending commands to all instances | `entrada-command` |
| `entrada.messaging.command.aws.retention` | AWS SQS retention period for command queue in seconds | `60` |
| `entrada.messaging.command.aws.visibility-timeout` | AWS SQS visibility timeout for command queue in seconds | `1` |
| `entrada.messaging.leader.name` | Queue name for leader container | `entrada-leader` |
| `entrada.messaging.leader.batchSize` | Leader queue batch size | `50` |
| `entrada.messaging.leader.aws.retention` | AWS SQS retention period for leader queue in seconds | `86400` |
| `entrada.messaging.leader.aws.visibility-timeout` | AWS SQS visibility timeout for leader queue in seconds | `300` |
| `entrada.leader` | Set to true for leader container (max 1) when using non-Kubernetes deployment | `false` |
| `entrada.provisioning.enabled` | Auto-create required components (bucket and queues) | `true` |

### Iceberg Configuration Options

The following table lists all configuration options starting with `iceberg.`:

| Option | Description | Default |
| ------ | ----------- | ------- |
| `iceberg.s3.access-key` | S3 access key for Iceberg operations (leave empty if using Lakekeeper vended credentials) | (empty) |
| `iceberg.s3.secret-key` | S3 secret key for Iceberg operations (leave empty if using Lakekeeper vended credentials) | (empty) |
| `iceberg.s3.region` | S3 region for Iceberg operations | `${AWS_REGION}` |
| `iceberg.s3.endpoint` | S3 endpoint (only when not using AWS) | (empty) |
| `iceberg.catalog.type` | Catalog type (rest, jdbc, or glue for AWS) | `rest` |
| `iceberg.catalog.uri` | REST catalog URI | (empty) |
| `iceberg.catalog.warehouse` | REST catalog warehouse identifier | (empty) |
| `iceberg.catalog.oauth2.uri` | OAuth2 token endpoint URI for REST catalog authentication | (empty) |
| `iceberg.catalog.oauth2.credential` | OAuth2 credential for REST catalog authentication | (empty) |
| `iceberg.catalog.oauth2.scope` | OAuth2 scope for REST catalog authentication | (empty) |
| `iceberg.catalog.warehouse-location` | S3 location for Iceberg warehouse | `s3://${entrada.s3.bucket}/${entrada.s3.warehouse-dir}` |
| `iceberg.catalog.host` | JDBC catalog host (PostgreSQL only) | (empty) |
| `iceberg.catalog.port` | JDBC catalog port | `5432` |
| `iceberg.catalog.name` | JDBC catalog database name | (empty) |
| `iceberg.catalog.user` | JDBC catalog username | (empty) |
| `iceberg.catalog.password` | JDBC catalog password | (empty) |
| `iceberg.compression` | Parquet compression codec (see Iceberg docs) | `zstd` |
| `iceberg.metadata.delete-after-commit` | Delete old metadata files after commit | `true` |
| `iceberg.metadata.version.max` | Maximum number of previous metadata versions to keep | `1` |
| `iceberg.table.name` | Iceberg table name | `dns` |
| `iceberg.table.namespace` | Iceberg table namespace | `entrada` |
| `iceberg.table.location` | S3 location for Iceberg table data | `s3://${entrada.s3.bucket}/${entrada.s3.warehouse-dir}/${iceberg.table.namespace}/${iceberg.table.name}` |
| `iceberg.parquet.dictionary-max-mb` | Maximum dictionary size in MB for Parquet encoding | `2` |
| `iceberg.parquet.bloomfilter` | Enable Bloom filter for dns_domainname column | `true` |
| `iceberg.parquet.min-records` | Minimum number of records required before closing Parquet output file | `10000000` |
| `iceberg.parquet.bloomfilter-max-size-mb` | Maximum Bloom filter size in MB | `1` |

### MaxMind GeoIP Configuration Options

The following table lists all configuration options starting with `maxmind.`:

| Option | Description | Default |
| ------ | ----------- | ------- |
| `maxmind.max-age-hr` | Max age in hours of local MaxMind database before downloading update | `24` |
| `maxmind.license.free` | MaxMind license key for free GeoLite2 databases | (empty) |
| `maxmind.license.paid` | MaxMind license key for paid GeoIP2 databases | (empty) |
| `maxmind.country.free` | Database name for free country database | `GeoLite2-Country` |
| `maxmind.country.paid` | Database name for paid country database | `GeoIP2-Country` |
| `maxmind.asn.free` | Database name for free ASN database | `GeoLite2-ASN` |
| `maxmind.asn.paid` | Database name for paid ASN/ISP database | `GeoIP2-ISP` |

### InfluxDB Metrics Configuration Options

The following table lists all configuration options starting with `management.influx.metrics.export.`:

| Option | Description | Default |
| ------ | ----------- | ------- |
| `management.influx.metrics.export.bucket` | InfluxDB bucket name for storing metrics | `entrada` |
| `management.influx.metrics.export.org` | InfluxDB organization name | `SIDN` |
| `management.influx.metrics.export.token` | InfluxDB authentication token | (empty) |
| `management.influx.metrics.export.uri` | InfluxDB server URI | (empty) |
| `management.influx.metrics.export.step` | Metrics export interval | `1m` |
| `management.influx.metrics.export.enabled` | Enable metrics export to InfluxDB | `false` |

JVM options (e.g., for memory limits) can be passed to the container using the JAVA_OPTS environment variable in the Docker Compose configuration:

```yaml
environment:
  - JAVA_OPTS=-Xmx4g -Xms4g
```

## AWS Deployment

When deployed on AWS, ENTRADA2 will automatically create a new database and table using AWS Glue. The data in the table can be analyzed using Athena, Spark or any of the other compatible tools available on AWS. See the AWS documentation for more details.

To enable running on AWS, set the `spring.cloud.aws.sqs.enabled` property to true to enable the use of SQS queues for messaging, otherwise RabbitMQ is used.  

## Kubernetes Deployment

When running ENTRADA2 on Kubernetes (on-premise or in cloud), all dependencies such as RabbitMQ must be deployed and available before using ENTRADA2. ENTRADA2 will automatically create a new database and table using the Iceberg JDBC catalog and it will also create the required messaging queues in RabbitMQ.

ENTRADA2 requires permission for creating and reading ConfigMaps for the leader election functionality. For example permissions, see: `k8s/leader-election-auth.yml`

## Rustfs S3 Configuration

The S3 implementation must send events to a RabbitMQ or AWS SQS queue when a new PCAP object has been uploaded. When not using AWS or the default Docker Compose configuration, the rustfs event configuration must be created manually.

### Creating a New Event Destination

The AMQP event destination for rustfs uses these options:

| Field name | Example |
| ---------- | ------- |
| URL | amqp://admin:${DEFAULT_PASSWORD}@rabbitmq-hostname:5672 |
| Exchange | entrada-s3-event-exchange |
| Exchange Type | direct |
| Routing Key | entrada-s3-event |
| Durable | Yes |

### Creating New Event Destination for a Bucket

Use the following values:

- **ARN**: The ARN for the above created event destination
- **Prefix**: The value of entrada option `entrada.s3.pcap-in-prefixes`
- **Suffix**: Optional suffix for PCAP files (e.g., pcap.gz)

## Scanning for New Objects

If the S3 storage solution used does not support events for newly uploaded objects, then enable scanning for new objects. Enable S3 object scanning by setting a value for the `entrada.schedule.new-object-min` property. The scanning feature will scan the S3 bucket every x minutes for new objects and will process the objects similar to how they would be processed when an event was created for the object.  

## API

A basic API is available for controlling the lifecycle of ENTRADA2 containers.

| Endpoint | Description |
| -------- | ----------- |
| PUT /api/v1/state/start | Start processing new PCAP files, the command will be relayed to all running containers |
| PUT /api/v1/state/stop | Stop processing new PCAP files, the command will be relayed to all running containers |
| PUT /api/v1/state/flush | Close open Parquet output files and add these to Iceberg table, the command will be relayed to all running containers |
| GET /api/v1/state | Get the current state of the container |

## Running Multiple Containers

Running multiple worker containers, all listening to the same S3 bucket events is possible and the correct method for scaling up. Make sure that only 1 container is the "leader", responsible for committing new data files to the Iceberg table.

When running on Kubernetes, leader election is performed automatically. When the leader container is shutdown, the leader election process will automatically select another container to become the leader.

When using Docker, you must set the `ENTRADA_LEADER` option to true only for the master container. There is no failover mechanism for master containers when using Docker.

## Table Schema

The column names use a prefix to indicate where the information was extracted from:

- `dns_`: from the DNS message
- `ip_`: from the IP packet
- `prot_`: from transport UDP/TCP
- `edns_`: from EDNS records in DNS message
- `tcp_`: from the TCP packet

| column name | type | Description |
| --- | --- | --- |
| dns_id | int | ID field from DNS header |
| time | long | Time of packet |
| dns_qname | string | qname from DNS question |
| dns_domainname | string | domainname from DNS question |
| ip_ttl | int | TTL of IP packet |
| ip_version | int | IP version used |
| prot | int | Protocol used (UDP or TCP) |
| ip_src | string | IP source address |
| prot_src_port | int | Source port |
| ip_dst | string | IP destination address |
| prot_dst_port | int | Destination port |
| dns_aa | boolean | AA flag from DNS header |
| dns_tc | boolean | TC flag from DNS header |
| dns_rd | boolean | RD flag from DNS header |
| dns_ra | boolean | RA flag from DNS header |
| dns_ad | boolean | AD flag from DNS header |
| dns_cd | boolean | CD flag from DNS header |
| dns_ancount | int | Answer RR count DNS |
| dns_arcount | int | Additional RR count DNS |
| dns_nscount | int | Authority RR count DNS |
| dns_qdcount | int | Question RR count DNS |
| dns_opcode | int | OPCODE from DNS header |
| dns_rcode | int | RCODE from DNS header |
| dns_qtype | int | QTYPE from DNS header |
| dns_qclass | int | CLASS from DNS question |
| ip_geo_country | string | Country for source IP address |
| ip_asn | string | ASN for source IP address |
| ip_asn_org | string | ASN organisation name for source IP address |
| edns_udp | int | UDP size from EDNS |
| edns_version | int | EDNS version |
| edns_do | boolean | DNSSEC OK flag |
| edns_options | array(int) | EDNS options from request |
| edns_ecs | string | EDNS Client Subnet |
| edns_ecs_ip_asn | string | ASN for IP address in ECS |
| edns_ecs_ip_asn_org | string | ASN organisation for IP address in ECS |
| edns_ecs_ip_geo_country | string | Country for IP address in ECS |
| edns_ext_error | array(int) | EDNS extended error |
| dns_labels | int | Number of labels in DNS qname |
| dns_proc_time | int | Time between request and response (millis) |
| dns_pub_resolver | string | Name of public resolver source |
| dns_req_len | int | Size of DNS request message |
| dns_res_len | int | Size of DNS response message |
| tcp_rtt | int | RTT (millis) based on TCP-handshake |
| server | string | Name of NS server |
| server_location | string | Name of anycast site of NS server |
| dns_rdata | array(rec) | rdata from response records |
| dns_rdata.section | int | Section from response (0=answer, 1=authoritative, 3=additional) |
| dns_rdata.type | int | Resource Record (RR) type, according to [IANA registration](https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-4) |
| dns_rdata.data | string | String representation of rdata part of RR |
| dns_cname | array(string) | CNAME records in the response chain |
| dns_tld | string | Top-Level Domain from the query name |
| dns_qname_full | string | Full query name without normalization |

Not all DNS resource records in ENTRADA have support for the `dns_rdata.data` column. For unsupported RRs, the value of this column will be null.

## Metrics

Metrics about the processed DNS data are generated when the configuration option `management.influx.metrics.export.enabled` is set to true. The metrics are sent to an [InfluxDB](https://www.influxdata.com/) instance, configured by the `management.influx.metrics.export.*` options.

## Components UI

Some of the components provide a web interface. Below are the URLs for the components started by the Docker Compose script. Login credentials can be found in the script (these are for test purposes only and do not constitute a security issue).

- [Rustfs](http://hostname:9000)
- [RabbitMQ](http://hostname:15672/)
- [InfluxDB](http://hostname:8086/)
- [Trino](http://hostname:8085/)

## License

This project is distributed under the GPLv3. See [LICENSE](LICENSE).

## Attribution

When building a product or service using ENTRADA2, we kindly request that you include the following attribution text in all advertising and documentation:

```html
This product includes ENTRADA2 created by <a href="https://www.sidnlabs.nl">SIDN Labs</a>, available from
<a href="http://entrada.sidnlabs.nl">http://entrada.sidnlabs.nl</a>.
```
