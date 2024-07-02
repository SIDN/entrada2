# ENTRADA2

A tool for converting captured DNS data (PCAP) to an [Apache Iceberg](https://iceberg.apache.org/) table, using the [Apache Parquet](https://parquet.apache.org/) dataformat.   
ENTRADA2 is an improvement on [ENTRADA](https://github.com/SIDN/entrada) and includes support for the multiple deployment types: Kubernetes, AWS and local deployment using Docker Compose.

A DNS query and response are combined into a single record and enriched by adding the following information.   
- Geolocation (Country)
- Autonomous system (ASN)
- Detection of public resolvers (Google, OpenDNS, Quad9 and Cloudflare)
- TCP round-trip time (RTT) 


ENTRADA2 supports:  
- S3 storage (MinIO, AWS)
- Open table format (Apache Iceberg)
- Messaging queuing (RabbitMQ, AWS SQS)
- Metadata Data catalog (AWS Glue or Iceberg JDBC)
- Metrics ([InfluxDB](https://www.influxdata.com/))
- Query engine (Trino, AWS Athena, Spark)

List of changes:

- Removed Hadoop support
- Improved support for s3 object storage
- Automaticly create required resources (s3 bucket and messaging queues)
- Added support for Kubernetes
- Added support for Apache Iceberg table format
- Split the QNAME into the dns_domainname and dns_qname (labels before domain name) columns
- Rows are sorted by domainname for more efficient compression
- Default column compression changed to ZSTD, was Snappy
- Bloomfilter for domainname column, performance boost when filtering on domainname 
- Use small Parquet max dictionary size, to prevent domainname column using dictionary and not Bloomfilter
- Renamed table columns to indicate protocol source
- Removed unused columns
- Name servers site pcaps are not processed by a dedicated container, a containers can handle any pcaps from any server
- No longer saving state between pcaps, might cause some unmatched packets
- Added s3 event based workflow
- Added API to control containers, e.g. status/start/stop
- Added option to include answer/authoritative/additional sections of DNS response in Parquet output


The following deployment modes are supported:
- Local/Single system using Docker Compose (best for test and evaluation)
- Kubernetes
- AWS

# Build

```
export TOOL_VERSION=0.0.3
mvn clean && mvn package && docker build  --tag=sidnlabs/entrada2:$TOOL_VERSION .
```

# GeoIP data
ENTRADA2 uses the [Maxmind](https://www.maxmind.com) GeoIP2 database, if you have a license then set the MAXMIND_LICENSE_PAID
environment variable, otherwise signup for the free [GeoLite2](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data ) database and use 
the MAXMIND_LICENSE_FREE environment variable.

# Quick start

To get started quickly, use the provided [Docker Compose script](https://github.com/SIDN/entrada2/blob/main/docker/docker-compose.yml) to create a test
environment on a single host, containing all required services. 

The configuration settings for the Docker Compose script can be found as environment variables in `docker.env`.  
The example uses the default Docker Compose script and starts 1 ENTRADA master container and 2 ENTRADA worker containers.  

```
export MAXMIND_LICENSE_PAID=<your-key-here>
docker compose --profile test up --scale entrada-worker=2
```

The Docker Compose script uses the "test" profile to start the ENTRADA containers, not using this profile will only start the dependencies and not the ENTRADA containers.

# Processing pcap data

ENTRADA2 converts a single pcap file into a 1 or more Parquet files, many small input pcap files may be automatically combined into a larger Parquet file.  
The `iceberg.parquet.min-records` option determines how many records a Parquet output file must contain before it is allowed to be closed and added to the Iceberg table.  

The pcap processing workflow uses 3 prefixes for pcap objects in s3, defaults are:
- pcap-in: New pcap object must be uploaed using this prefix
- pcap-done: When the auto-delete option is not enabled, then processed pcap objects are moved to this prefix
- pcap-failed: When a pcap object cannot be processed correcly, it is moved to this prefix

When all components have started up, you may upload a pcap file to the s3 bucket, using the "pcap-in" prefix.
If ENTRADA2 created a s3 bucket notification configuration (enabled by default) then processing of the a pcap file 
will automatically start.  

Use the the following s3 tags when uploading file to S3:

- entrada-ns-server: Logical name of the name server (e.g. ns1.dns.nl)
- entrada-ns-anycast-site: Anycast site of the name server (e.g. ams)


# Flushing output
To prevent many small Parquet output files when the input stream contains many small pcap files, there exists a configuration option (`iceberg.parquet.min-records`, default 1000000) for setting the lower limit of the number
of records in a Parquet output file. Output files are not closed and added to the table until this limit is reached. To force closing the output file(s), e.g. when doing maintenance, use the "flush" API endpoint.
``PUT https://hostname:8080/api/v1/state/flush` will cause all current open files to be closed and added to the table.


# Analysing results

The results may be analysed using different tools, such as AWS Athena, Trino or Apache Spark. 
The Docker Compose script automaticlly starts Trino, for quickly analysing a limited dataset.  

Example using the Trino commandline client (installed in Trino container):

```
docker exec -it docker-trino-1 trino
```

Switch to the correct catalog and namespace:

```
use iceberg.entrada2;
```

Query data in dns table, for example count rows in the dns table like this:

```
select count(1)
from dns;
```

Find out what domain name received most queries, create a top 25:

```
select dns_domainname, count(1)
from dns
group by 1
order by 2 desc
limit 25;
```

When response data is enabled, the rdata column can be queried using lambda expressions, for example to get all queries for a specific rdata response value:

```
select dns_qname, dns_domainname, dns_rdata 
from dns 
where  any_match(dns_rdata, r -> r.data = '185.159.199.200') and dns_domainname = 'dns.nl'
limit 100;
```

Using fields from the rdata column, for example get top 25 by A-records in reponses.

```
select d.data, count(1)
from dns, UNNEST(dns.dns_rdata) d
where d.type = 1 
group by 1 
order by 2 desc  
limit 25;
```

# Cleanup
To cleanup the test evironment, stop the Docker containers, delete the Docker volumes and restart the containers.

```
docker compose down
docker system prune -f
docker volume rm docker_entradaInfluxDbVolume docker_entradaMinioVolume docker_entradaPostgresqlVolume;
docker compose --profile test up --scale entrada-worker=2
```

# Configuration
The ENTRADA2 configuration options are in the [Spring Boot configuration file](https://raw.githubusercontent.com/SIDN/entrada2/main/src/main/resources/application.yml)
All options in this file can be overidden by using environment variables, or in the case of Kubernetes you can also create a ConfigMap containing a custom configuration file.

# AWS

When deployed on AWS, ENTRADA2 will automatically create a new database and table using AWS Glue, the data in the table can be analyzed using Athena, Spark or any of the other compatible tools available on AWS.
See the AWS documentation for more details.
To enable running on AWS, set the `spring.cloud.aws.sqs.enabled` property to true to enable the use of SQS queues for messaging, otherwise RabbitMQ is used.  


# Kubernetes
When running ENTRADA2 on Kubernetes (onpremise or in cloud) all dependencies such as RabbitMQ must be deployed and available before using ENTRADA2.  
ENTRADA2 will automatically create a new database and table using the Iceberg JDBC catalog and it will also create the required messaging queues in RabbitMq.

ENTRADA2 requires permission for creating and reading ConfigMaps, for the Leader election functionality.
For example permissions see: `k8s/leader-election-auth.yml`

# MinIO S3
The S3 implementation must send events to a RabbitMQ or AWS SQS queue when a new pcap object has been uploaded.
When not using AWS or the default Docker Compose configuration, the MinIO event configuration must be created manually.

## Creating a new Event Destination

the AMQP event destination for MinIO uses these options:

| Field name  |  Example   |
| ------------ | --------------- |
| URL       | amqp://admin:${DEFAULT_PASSWORD}@rabbitmq-hostname:5672  |
| Exchange  |  entrada-s3-event-exchange | 
| Exchange Type  | direct  | 
| Routing Key  | entrada-s3-event  | 
| Durable  | Yes  | 


## Creating new Event destination for a Bucket

Use the following values:

- ARN: The ARN for the above created event destination
- Prefix: The value of entrada option `entrada.s3.pcap-in-dir` default is `pcap-in`
- Suffix: Opional suffix for pcap files e.g. pcap.gz


# API

A basic API is available for controlling the lifecycle of ENTRADA2 containers.

| Endpoint    | Description   |
| ------------ | ----------- | 
| PUT /api/v1/state/start  |  Start processing new pcap file, the command will be relayed to all running containers        |
| PUT /api/v1/state/stop  | Stop processing new pcap file, the command will be relayed to all running containers    
| PUT /api/v1/state/flush  | Close open Parquet output files and add these to Iceberg table, the command will be relayed to all running containers        |
| GET /api/v1/state  | Get the current state of the container       |


# Running multiple containers

Running multiple worker containers, all listening to the same s3 bucket events is possible and the correct method for scaling up.
Make sure that only 1 container is the "leader", responsible for comitting new datafiles to the Iceberg table.  

When running on Kubernetes, leader election is performed automatically. When the leader container is shutdown, the leader election process will automatically select another container to become the leader.  
When using Docker you must set the `ENTRADA_LEADER` option to true only for the master container, there is no failover mechanism for master containers when using Docker.


# Table schema

The column names use a prefix to indicate where the information was extracted from:

- dns_ : from the DNS message
- ip_: from the IP packet
- prot_: from transport UDP/TCP
- edns_: from EDNS records in DNS message 
- tcp_: from the TCP packet

| column name  | type   | Description               |
| ------------ | ------ | ------------------------- |
| dns_id       | int    | ID field from DNS header  |
| time         | long    | Time of packet  |
| dns_qname       | string    | qname from DNS question  |
| dns_domainname       | string    | domainname from DNS question  |
| ip_ttl       | int    | TTL of IP packet  |
| ip_version       | int    | IP version used  |
| prot       | int    | Protocool used (UDP or TCP  |
| ip_src       | string    | IP source address  |
| prot_src_port       | int    | Source port  |
| ip_dst       | string    | IP destination address  |
| prot_src_port|   string    | Destination port |
| dns_aa       | boolean    | AA flag from DNS header  |
| dns_tc       | boolean    | TC flag from DNS header  |
| dns_rd       | boolean    | RD flag from DNS header  |
| dns_ra       | boolean    | RA flag from DNS header  |
| dns_ad       | boolean    | AD flag from DNS header  |
| dns_cd       | boolean    | CD flag from DNS header  |
| dns_ancount       | int    | Answer RR count DNS  |
| dns_arcount       | int    | Additional RR count DNS   |
| dns_nscount       | int    | Authority RR count DNS   |
| dns_qdcount       | int    | Question RR count DNS   |
| dns_opcode       | int    | OPCODE from DNS header  |
| dns_rcode       | int    | RCODE from DNS header  |
| dns_qtype       | int    | QTYPE from DNS header  |
| dns_qclass       | int    | CLASS from DNS question  |
| ip_geo_country       | string    | Country for source IP adress  |
| ip_asn       | string    | ASN for source IP adress  |
| ip_asn_org       | string    | ASN organisation name for source IP adress  |
| edns_udp       | int    | UDP size from EDNS  |
| edns_version       | int    | EDNS version  |
| edns_options       | array(int)    | EDNS options from request |
| edns_ecs       | string    | EDNS Client Subnet   |
| edns_ecs_ip_asn       | int    | ASN for IP address in ECS   |
| edns_ecs_ip_asn_org       | int    | ASN organisation for IP address in ECS   |
| edns_ecs_ip_geo_country       | int    | Country for IP address in ECS   |
| edns_ext_error       | array(int)    | EDNS extended error   |
| dns_labels       | int    | Number of labels in DNS qname   |
| dns_proc_time       | int    | Time between request and response (millis)   |
| dns_pub_resolver       | string    | Name of public resolver source   |
| dns_req_len       | int    | Size of DNS request message  |
| dns_res_len       | int    | Size of DNS response message  |
| tcp_rtt       | int    | RTT (millis) based on TCP-handshake  |
| server       | string    | Name of NS server  |
| server_location       | string    | Name of anycast site of NS server  |
| dns_rdata       | array(rec)  | rdata from response records  |
| dns_rdata.section     | int   | Section from response (0=answer, 1=authoritative, 3=additional |
| dns_rdata.type     | int   | Resource Record (RRR) type, according to [IANA registration](https://www.iana.org/assignments/dns-parameters/dns-parameters.xhtml#dns-parameters-4) |
| dns_rdata.data     | string   | String representation of rdata part of RR|

Not all records include support for the dns_rdata.data field, in this cace the value of the field will be null.

# Metrics

Metrics about the processed DNS data are generated when the configuration option `management.influx.metrics.export.enabled` is set to true.
The metrics are sent to an [InfluxDB](https://www.influxdata.com/) instance, configured by the `management.influx.metrics.export.*` options.


# Components UI
Some of the components provide a web interface, below are the URLs for the components started by the docker compose script.
Login credentials can be found in the script.

- [MinIO](http://hostname:9000)
- [RabbitMQ](http://hostname:15672/)
- [InfluxDB](http://hostname:8086/)
- [Trino](http://hostname:8085/) 


# License

This project is distributed under the GPLv3, see [LICENSE](LICENSE).

# Attribution

When building a product or service using ENTRADA2, we kindly request that you include the following attribution text in all advertising and documentation.
```
This product includes ENTRADA2 created by <a href="https://www.sidnlabs.nl">SIDN Labs</a>, available from
<a href="http://entrada.sidnlabs.nl">http://entrada.sidnlabs.nl</a>.
```

