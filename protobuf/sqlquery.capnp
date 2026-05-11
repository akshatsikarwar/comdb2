@0xc9c2d6db83a19363;

# Cap'n Proto schema for Comdb2 newsql query messages.
# Mirrors sqlquery.proto for the Cap'n Proto wire protocol path.

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("cdb2capnp");

enum RequestType {
  unknown     @0;
  cdb2Query   @1;  # proto: CDB2QUERY = 1
  reset       @2;  # proto: RESET = 108
  sslconn     @3;  # proto: SSLCONN = 121
}

enum ClientFeature {
  unknown             @0;
  skipIntransResults  @1;
  allowMasterExec     @2;
  allowMasterDbinfo   @3;
  allowQueuing        @4;
  ssl                 @5;
  flatColVals         @6;
  requestFp           @7;
  sqliteRowFormat     @8;  # proto value 9 (8 maps to ordinal 9 on decode)
  requireFastsql      @9;
  canRedirectFdb      @10;
  allowIncoherent     @11;
}

enum DistOp {
  unknown       @0;
  prepare       @1;
  discard       @2;
  prepared      @3;
  failedPrepare @4;
  commit        @5;
  abort         @6;
  propagated    @7;
  heartbeat     @8;
}

struct Flag {
  option @0 :Int32;
  value  @1 :Int32;
}

struct SnapshotInfo {
  file   @0 :Int32;
  offset @1 :Int32;
}

struct ClientInfo {
  pid              @0 :Int32;
  thId             @1 :UInt64;
  hostId           @2 :Int32;
  argv0            @3 :Text;
  stack            @4 :Text;
  apiDriverName    @5 :Text;
  apiDriverVersion @6 :Text;
}

struct ReqInfo {
  timestampus @0 :Int64;
  numRetries  @1 :Int32;
}

struct IdentityBlob {
  principal    @0 :Text;
  majorVersion @1 :Int64;
  minorVersion @2 :Int64;
  data         @3 :Data;
}

struct I32Array {
  elements @0 :List(Int32);
}

struct I64Array {
  elements @0 :List(Int64);
}

struct DblArray {
  elements @0 :List(Float64);
}

struct TxtArray {
  elements @0 :List(Text);
}

struct BlobArray {
  elements @0 :List(Data);
}

struct BindArray {
  union {
    none @0 :Void;
    i32  @1 :I32Array;
    i64  @2 :I64Array;
    dbl  @3 :DblArray;
    txt  @4 :TxtArray;
    blob @5 :BlobArray;
  }
}

struct BindValue {
  varname @0 :Text;
  type    @1 :Int32;
  value   @2 :Data;
  isnull  @3 :Bool = false;
  index   @4 :Int32 = -1;
  carray  @5 :BindArray;
}

struct SqlQuery {
  dbname       @0 :Text;
  sqlQuery     @1 :Text;
  flags        @2 :List(Flag);
  littleEndian @3 :Bool;
  bindvars     @4 :List(BindValue);
  tzname       @5 :Text;
  setFlags     @6 :List(Text);
  types        @7 :List(Int32);
  machClass    @8 :Text = "unknown";
  cnonce       @9 :Data;
  snapshotInfo @10 :SnapshotInfo;
  skipRows     @11 :Int64 = 0;
  retry        @12 :Int32 = 0;
  features     @13 :List(Int32);
  clientInfo   @14 :ClientInfo;
  context      @15 :List(Text);
  reqInfo      @16 :ReqInfo;
  identity     @17 :IdentityBlob;
  isTagged     @18 :Bool = false;
}

struct DbInfo {
  dbname       @0 :Text;
  littleEndian @1 :Bool;
  wantEffects  @2 :Bool = false;
}

struct DistTxnOp {
  operation @0 :Int32;
  async     @1 :Bool;
  txnid     @2 :Text;
  name      @3 :Text;
  tier      @4 :Text;
  master    @5 :Text;
  rcode     @6 :Int32 = 0;
  outrc     @7 :Int32 = 0;
  errmsg    @8 :Text;
}

struct DistTxn {
  dbname   @0 :Text;
  disttxn  @1 :DistTxnOp;
}

struct Query {
  union {
    none     @0 :Void;
    sqlquery @1 :SqlQuery;
    dbinfo   @2 :DbInfo;
    spcmd    @3 :Text;
    disttxn  @4 :DistTxn;
  }
}
