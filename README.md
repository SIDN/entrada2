# ENTRADA2

For converting captured DNS data to an [Apache Parquet](https://parquet.apache.org/) format based [Apache Iceberg](https://iceberg.apache.org/) table.   
ENTRADA2 is an improvement on [ENTRADA](https://github.com/SIDN/entrada) and is designed to be more performent, scalable and to reduce the output size of the generated Parquet data.
ENTRADA2 uses a new table schema and is not compatible with the original ENTRADA table schema.

The data is enriched by adding the following details to each row.   
- Geolocation (Country)
- Autonomous system (ASN) details
- Detection of public resolvers (Google, OpenDNS, Quad9 and Cloudflare)
- TCP round-trip time (RTT) 


Based on the following components:  

- S3 storage (MinIO, AWS)
- Messaging Queue ( RabbitMQ, AWS SQS)
- Metadata Data catalog (AWS Glue or [REST based Iceberg catalog server](https://github.com/SIDN/iceberg-rest-catalog-server) + PostgreSQL)
- Metrics (InfluxDB)
- Query engine (Trino, AWS Athena, Spark)

List of changes:

- Removed Hadoop support
- Improved support for s3 object storage
- Automaticly create required resources (s3 bucks and messaging queues)
- Added support for Kubernetes
- Added support for Apache Iceberg table format
- The dns_qname column only contains the labels preceding the domainname
- Rows are now sorted by domainname for more efficient compression
- Default column compression changed to gzip, was Snappy
- Use small Parquet max dictionary size, to prevent domainname column using dict
- Use bloomfilter for domainname column, performance boost when filtering on domainname 
- Renamed table columns to indicate protocol
- Removed unused columns
- No longer required to coinfgure name server, container can handle any pcap
- No longer storing state between pcaps, might cause some unmatched packets
- Added event based workflow, started after uploading pcap to s3
- Added API to control containers, e.g. start/stop processing


The following deployment modes are supported:
- Docker (best for test and evaluation)
- Kubernetes
- AWS cloud

# Build

```
mvn package
docker build --tag=sidnlabs/entrada2:0.0.1 .
docker push sidnlabs/entrada2:0.0.1
```

# Getting started

If you want to get started, an easy method is by using the [Docker Compose script](https://github.com/SIDN/entrada2/blob/main/docker/docker-compose.yml) to create a test
environment containing all the required components, using default configuration settings.
ENTRADA2 uses the [Maxmind](https://www.maxmind.com) GeoIP2 database, if you have a license then set the MAXMIND_LICENSE_PAID
environment variable, otherwise signup for the free [GeoLite2](https://dev.maxmind.com/geoip/geolite2-free-geolocation-data ) database and use 
the MAXMIND_LICENSE_FREE environment variable.

Example using the Maxmind GeoIP2 database:

```
export MAXMIND_LICENSE_PAID=<your-key-here>
docker-compose --profile test up
```

## Uploading pcap file
When all components have started up, you may upload a pcap file to s3, processing of the new pcap file will start automatically.  
The default bucket name is `sidnlabs-iceberg-data` and pcap files need to be uploaded to the directory `pcap/`.  

Use the the following s3 tags when uploading file to S3:

- entrada-ns-server: Logical name of the name server
- entrada-ns-anycast-site: Anycast site of the name server

Example using MinIO:  

```
# connect to Minio Docker container
docker exec -it docker-minio-1 /bin/bash

# create a new minio alias for use with mc command
mc alias set minio http://minio:9000 entrada <get password from docker-compose file>

# goto pcap directory
# The /pcap directory in the Minio container is mapped to a pcap sub-directory on the host.  
cd /pcap

# upload a pcap (add pcap file first to directory on host)
mc cp --tags "entrada-ns-server=ns1.example.nl&entrada-ns-anycast-site=ams"  \
ams-ns1-150_2023-10-04-11:09:51.pcap.gz minio/sidnlabs-iceberg-data/pcap/ams-ns1-150_2023-10-04-11:09:51.pcap.gz
```

After uploading the pcap file, see the docker logging for information about the processed file. 


## Analysing results
The results may be analysed using different tools, such as AWS Athena, Trino or Apache Spark. 
The Docker Compose script automaticlly starts Trino, for quickly analysing a limited dataset.  

Example using the Trino commandline client (installed in Trino container):

```
docker exec -it docker-trino-1 trino
```

Switch to the correct catalog:

```
use iceberg_entrada.entrada2
```

Query data in dns table:

```
select count(1) from dns;
```

## Cleanup
To cleanup the test evironment, stop the Docker containers, delete the Docker volumes and restart the containers.

```
docker system prune -f
docker volume rm docker_dataVolume
docker volume rm docker_pgVolume
docker-compose --profile test up
```


## API

A basic API is available for controlling the lifecycle of ENTRADA2 containers.

| Endpoint    | Description   |
| ------------ | ----------- | 
| PUT /api/v1/state/start  |  Start processing new pcap file, the command will be relayed to all running containers        |
| PUT /api/v1/state/stop  | Stop processing new pcap file, the command will be relayed to all running containers        |
| GET /api/v1/state  | Get the current state of the container       |


## Running multiple containers

TODO

## Table schema

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
| edns_ext_error       | int    | EDNS extended error   |
| dns_labels       | int    | Number oif labels in DNS qname   |
| dns_proc_time       | int    | Time between request and response (millis)   |
| dns_pub_resolver       | string    | Name of public resolver source   |
| dns_req_len       | int    | Size of DNS request message  |
| dns_res_len       | int    | Size of DNS response message  |
| tcp_rtt       | int    | RTT (millis) based on TCP-handshake  |
| server       | string    | Name of NS server  |
| server_location       | string    | Name of anycast site of NS server  |


## Metrics

TODO


## Components UI
Some of the components provide a web interface, below are the URLs for the components started by the docker compose script.
Login credentials can be found in the script.

- [MinIO](http://localhost:9000)
- [RabbitMQ](http://localhost:15672/)
- [InfluxDB](http://localhost:8086/)
- [Trino](http://localhost:8085/) 


## License

This project is distributed under the GPLv3, see [LICENSE](LICENSE).

## Attribution

When building a product or service using ENTRADA2, we kindly request that you include the following attribution text in all advertising and documentation.
```
This product includes ENTRADA2 created by <a href="https://www.sidnlabs.nl">SIDN Labs</a>, available from
<a href="http://entrada.sidnlabs.nl">http://entrada.sidnlabs.nl</a>.
```

