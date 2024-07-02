package nl.sidn.entrada2.load;

// ENUM for fast access to the index of a record field, this saves an additional lookup to
// find index for field name.
public enum RdataFieldEnum {
  dns_rdata_section,
  dns_rdata_type,
  dns_rdata_data;
}
