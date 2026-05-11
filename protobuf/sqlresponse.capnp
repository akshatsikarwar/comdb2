@0xfa753ccc611455b4;

# Cap'n Proto schema for Comdb2 newsql response messages.
# Mirrors sqlresponse.proto for the Cap'n Proto wire protocol path.

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("cdb2capnp");

enum ColumnType {
  unknown      @0;
  integer      @1;
  real         @2;
  cstring      @3;
  blob         @4;
  datetime     @5;  # proto: DATETIME = 6 (no gap in capnp ordinals)
  intervalym   @6;
  intervalds   @7;
  datetimeus   @8;
  intervaldsus @9;
}

enum ErrorCode {
  ok                @0;
  dupOld            @1;
  connectError      @2;
  notconnected      @3;
  prepareError      @4;
  prepareErrorOld   @5;
  ioError           @6;
  internal          @7;
  nostatement       @8;
  badcolumn         @9;
  badstate          @10;
  asyncerr          @11;
  okAsync           @12;
  invalidId         @13;
  recordOutOfRange  @14;
  rejected          @15;
  stopped           @16;
  badreq            @17;
  dbcreateFailed    @18;
  threadpoolInternal @19;
  readonly          @20;
  appsockLimit      @21;
  nomaster          @22;
  notserial         @23;
  schemachange      @24;
  untaggedDatabase  @25;
  constraints       @26;
  deadlock          @27;
  tranIoError       @28;
  access            @29;
  querylimit        @30;
  masterTimeout     @31;
  wrongDb           @32;
  verifyError       @33;
  fkeyViolation     @34;
  nullConstraint    @35;
  convFail          @36;
  nonkless          @37;
  malloc            @38;
  notsupported      @39;
  tranTooBig        @40;
  duplicate         @41;
  tznameFail        @42;
  changenode        @43;
  incomplete        @44;
  unknown           @45;
}

enum ResponseHeader {
  unknown                 @0;
  sqlResponseHeartbeat    @1;  # proto: 205
  sqlResponse             @2;  # proto: 1002
  dbinfoResponse          @3;  # proto: 1005
  sqlEffects              @4;  # proto: 1006
  sqlResponsePing         @5;  # proto: 1007
  sqlResponsePong         @6;  # proto: 1008
  sqlResponseTrace        @7;  # proto: 1009
  sqlResponseSsl          @8;  # proto: 1010
  disttxnResponse         @9;  # proto: 1011
  sqlResponseRaw          @10; # proto: 1012
}

enum ResponseType {
  unknown      @0;
  columnNames  @1;
  columnValues @2;
  lastRow      @3;
  comdb2Info   @4;
  spTrace      @5;
  spDebug      @6;
  sqlRow       @7;
  rawData      @8;
}

enum SyncMode {
  unknown    @0;
  sync       @1;
  async      @2;
  syncRoom   @3;
  syncN      @4;
  syncSource @5;
  syncUnknown @6;
}

enum ServerFeature {
  unknown            @0;
  skipIntransResults @1;
}

struct NodeInfo {
  name        @0 :Text;
  number      @1 :Int32;
  incoherent  @2 :Int32;
  room        @3 :Int32 = 0;
  port        @4 :Int32 = 0;
}

struct DbInfoResponse {
  master     @0 :NodeInfo;
  nodes      @1 :List(NodeInfo);
  requireSsl @2 :Bool = false;
  syncMode   @3 :Int32 = 0;  # CDB2SyncMode values: SYNC=1, ASYNC=2...
}

struct DistTxnResponse {
  rcode @0 :Int32;
}

struct Effects {
  numAffected @0 :Int32;
  numSelected @1 :Int32;
  numUpdated  @2 :Int32;
  numDeleted  @3 :Int32;
  numInserted @4 :Int32;
}

struct Column {
  type   @0 :Int32;  # CDB2ColumnType values: INTEGER=1, REAL=2, CSTRING=3, BLOB=4, DATETIME=6..
  value  @1 :Data;
  isnull @2 :Bool = false;
}

struct ResponseSnapshotInfo {
  file   @0 :Int32;
  offset @1 :Int32;
}

struct SqlResponse {
  responseType      @0  :ResponseType;
  value             @1  :List(Column);
  dbinforesponse    @2  :DbInfoResponse;
  errorCode         @3  :Int32;   # CDB2ErrorCode values passed through as-is (can be negative)
  errorString       @4  :Text;
  effects           @5  :Effects;
  snapshotInfo      @6  :ResponseSnapshotInfo;
  rowId             @7  :UInt64 = 0;
  features          @8  :List(Int32);
  infoString        @9  :Text;
  flatColVals       @10 :Bool = false;
  values            @11 :List(Data);
  isnulls           @12 :List(Bool);
  fp                @13 :Data;
  sqliteRow         @14 :Data;
  foreignDb         @15 :Text;
  foreignClass      @16 :Text;
  foreignPolicyFlag @17 :Int32 = 0;
  disttxnresponse   @18 :DistTxnResponse;
  sqlTailOffset     @19 :Int32 = 0;
}

struct SqlResponseIgnoreData {
  responseType @0 :ResponseType;
  errorCode    @1 :Int32;  # CDB2ErrorCode
}
