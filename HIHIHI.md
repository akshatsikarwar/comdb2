  How to activate Cap'n Proto:
  CDB2_USE_CAPNPROTO=1 cdb2sql testdb local "SELECT 1"
  The server auto-detects from newsqlheader.type == 3 and mirrors the protocol for all responses. Existing protobuf clients continue to work unchanged.

