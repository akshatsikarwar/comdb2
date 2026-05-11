/*
 * Cap'n Proto codec implementation.
 *
 * Converts between Cap'n Proto wire format (used when newsqlheader.type ==
 * CAPNP_CDB2QUERY) and the protobuf-c C struct types that the rest of the
 * codebase works with internally.
 *
 * All heap allocations in the unpack functions use plain malloc/strdup so
 * that cdb2__xxx__free_unpacked(msg, NULL) (NULL = system allocator) can
 * free them correctly.  The pack functions return a malloc'd buffer that the
 * caller must free() directly.
 */

#include "capnp_codec.h"

#include <capnp/message.h>
#include <capnp/serialize.h>
#include <kj/array.h>
#include <kj/memory.h>

#include "sqlquery.capnp.h"
#include "sqlresponse.capnp.h"

/* protobuf-c struct definitions and free_unpacked functions */
#include "sqlquery.pb-c.h"
#include "sqlresponse.pb-c.h"

#include <stdlib.h>
#include <string.h>

/* ------------------------------------------------------------------ */
/* Helpers                                                             */
/* ------------------------------------------------------------------ */

static char *xstrdup(const char *s)
{
    return s ? strdup(s) : nullptr;
}

static uint8_t *xmemdup(const void *p, size_t len)
{
    if (!p || len == 0) return nullptr;
    uint8_t *out = (uint8_t *)malloc(len);
    if (out) memcpy(out, p, len);
    return out;
}

/* Serialize a MessageBuilder to a malloc'd byte buffer. */
static size_t builder_to_bytes(capnp::MessageBuilder &builder, uint8_t **outbuf)
{
    kj::Array<capnp::word> words = capnp::messageToFlatArray(builder);
    kj::ArrayPtr<const kj::byte> bytes = words.asBytes();
    size_t sz = bytes.size();
    *outbuf = (uint8_t *)malloc(sz);
    if (*outbuf) memcpy(*outbuf, bytes.begin(), sz);
    return *outbuf ? sz : 0;
}

/* ------------------------------------------------------------------ */
/* ResponseType: protobuf value == capnp ordinal (both start at 1)    */
/* ------------------------------------------------------------------ */

static cdb2capnp::ResponseType to_capnp_rt(int32_t v)
{
    return static_cast<cdb2capnp::ResponseType>(v);
}
static int32_t from_capnp_rt(cdb2capnp::ResponseType v)
{
    return static_cast<int32_t>(v);
}

/* ------------------------------------------------------------------ */
/* Unpack helpers: Cap'n Proto → protobuf-c structs                   */
/* ------------------------------------------------------------------ */

static CDB2SQLQUERY__Snapshotinfo *
unpack_snapshotinfo_q(cdb2capnp::SnapshotInfo::Reader r)
{
    CDB2SQLQUERY__Snapshotinfo *s =
        (CDB2SQLQUERY__Snapshotinfo *)malloc(sizeof(CDB2SQLQUERY__Snapshotinfo));
    cdb2__sqlquery__snapshotinfo__init(s);
    s->file   = r.getFile();
    s->offset = r.getOffset();
    return s;
}

static CDB2SQLQUERY__Cinfo *
unpack_cinfo(cdb2capnp::ClientInfo::Reader r)
{
    CDB2SQLQUERY__Cinfo *c =
        (CDB2SQLQUERY__Cinfo *)malloc(sizeof(CDB2SQLQUERY__Cinfo));
    cdb2__sqlquery__cinfo__init(c);
    c->pid             = r.getPid();
    c->th_id           = r.getThId();
    c->host_id         = r.getHostId();
    c->argv0           = xstrdup(r.getArgv0().cStr());
    c->stack           = xstrdup(r.getStack().cStr());
    c->api_driver_name    = xstrdup(r.getApiDriverName().cStr());
    c->api_driver_version = xstrdup(r.getApiDriverVersion().cStr());
    return c;
}

static CDB2SQLQUERY__Reqinfo *
unpack_reqinfo(cdb2capnp::ReqInfo::Reader r)
{
    CDB2SQLQUERY__Reqinfo *ri =
        (CDB2SQLQUERY__Reqinfo *)malloc(sizeof(CDB2SQLQUERY__Reqinfo));
    cdb2__sqlquery__reqinfo__init(ri);
    ri->timestampus = r.getTimestampus();
    ri->num_retries = r.getNumRetries();
    return ri;
}

static CDB2SQLQUERY__IdentityBlob *
unpack_identity(cdb2capnp::IdentityBlob::Reader r)
{
    CDB2SQLQUERY__IdentityBlob *id =
        (CDB2SQLQUERY__IdentityBlob *)malloc(sizeof(CDB2SQLQUERY__IdentityBlob));
    cdb2__sqlquery__identity_blob__init(id);
    id->principal    = xstrdup(r.getPrincipal().cStr());
    id->majorversion = r.getMajorVersion();
    id->minorversion = r.getMinorVersion();
    auto d = r.getData();
    id->data.len  = d.size();
    id->data.data = xmemdup(d.begin(), d.size());
    return id;
}

/* Copy a Cap'n Proto primitive List into a freshly malloc'd C array. */
template <typename T, typename List>
static T *copy_prim_list(List els)
{
    if (els.size() == 0) return nullptr;
    T *out = (T *)malloc(els.size() * sizeof(T));
    for (uint32_t i = 0; i < els.size(); ++i)
        out[i] = (T)els[i];
    return out;
}

static CDB2SQLQUERY__Bindvalue__Array *
unpack_bind_array(cdb2capnp::BindArray::Reader r)
{
    CDB2SQLQUERY__Bindvalue__Array *ba =
        (CDB2SQLQUERY__Bindvalue__Array *)malloc(
            sizeof(CDB2SQLQUERY__Bindvalue__Array));
    cdb2__sqlquery__bindvalue__array__init(ba);

    switch (r.which()) {
    case cdb2capnp::BindArray::I32: {
        auto els = r.getI32().getElements();
        CDB2SQLQUERY__Bindvalue__I32Array *arr =
            (CDB2SQLQUERY__Bindvalue__I32Array *)malloc(
                sizeof(CDB2SQLQUERY__Bindvalue__I32Array));
        cdb2__sqlquery__bindvalue__i32_array__init(arr);
        arr->n_elements = els.size();
        arr->elements   = copy_prim_list<int32_t>(els);
        ba->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I32;
        ba->i32 = arr;
        break;
    }
    case cdb2capnp::BindArray::I64: {
        auto els = r.getI64().getElements();
        CDB2SQLQUERY__Bindvalue__I64Array *arr =
            (CDB2SQLQUERY__Bindvalue__I64Array *)malloc(
                sizeof(CDB2SQLQUERY__Bindvalue__I64Array));
        cdb2__sqlquery__bindvalue__i64_array__init(arr);
        arr->n_elements = els.size();
        arr->elements   = copy_prim_list<int64_t>(els);
        ba->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I64;
        ba->i64 = arr;
        break;
    }
    case cdb2capnp::BindArray::DBL: {
        auto els = r.getDbl().getElements();
        CDB2SQLQUERY__Bindvalue__DblArray *arr =
            (CDB2SQLQUERY__Bindvalue__DblArray *)malloc(
                sizeof(CDB2SQLQUERY__Bindvalue__DblArray));
        cdb2__sqlquery__bindvalue__dbl_array__init(arr);
        arr->n_elements = els.size();
        arr->elements   = copy_prim_list<double>(els);
        ba->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_DBL;
        ba->dbl = arr;
        break;
    }
    case cdb2capnp::BindArray::TXT: {
        auto els = r.getTxt().getElements();
        CDB2SQLQUERY__Bindvalue__TxtArray *arr =
            (CDB2SQLQUERY__Bindvalue__TxtArray *)malloc(
                sizeof(CDB2SQLQUERY__Bindvalue__TxtArray));
        cdb2__sqlquery__bindvalue__txt_array__init(arr);
        arr->n_elements = els.size();
        if (els.size()) {
            arr->elements = (char **)malloc(els.size() * sizeof(char *));
            for (uint32_t i = 0; i < els.size(); ++i)
                arr->elements[i] = xstrdup(els[i].cStr());
        }
        ba->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_TXT;
        ba->txt = arr;
        break;
    }
    case cdb2capnp::BindArray::BLOB: {
        auto els = r.getBlob().getElements();
        CDB2SQLQUERY__Bindvalue__BlobArray *arr =
            (CDB2SQLQUERY__Bindvalue__BlobArray *)malloc(
                sizeof(CDB2SQLQUERY__Bindvalue__BlobArray));
        cdb2__sqlquery__bindvalue__blob_array__init(arr);
        arr->n_elements = els.size();
        if (els.size()) {
            arr->elements = (ProtobufCBinaryData *)malloc(
                els.size() * sizeof(ProtobufCBinaryData));
            for (uint32_t i = 0; i < els.size(); ++i) {
                auto d = els[i];
                arr->elements[i].len  = d.size();
                arr->elements[i].data = xmemdup(d.begin(), d.size());
            }
        }
        ba->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_BLOB;
        ba->blob = arr;
        break;
    }
    default:
        ba->type_case = CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE__NOT_SET;
        break;
    }
    return ba;
}

static CDB2SQLQUERY *
unpack_sqlquery(cdb2capnp::SqlQuery::Reader r)
{
    CDB2SQLQUERY *sq = (CDB2SQLQUERY *)malloc(sizeof(CDB2SQLQUERY));
    cdb2__sqlquery__init(sq);

    sq->dbname    = xstrdup(r.getDbname().cStr());
    sq->sql_query = xstrdup(r.getSqlQuery().cStr());
    sq->little_endian = r.getLittleEndian();

    /* flags */
    auto flags = r.getFlags();
    sq->n_flag = flags.size();
    if (sq->n_flag) {
        sq->flag = (CDB2FLAG **)malloc(sq->n_flag * sizeof(CDB2FLAG *));
        for (uint32_t i = 0; i < sq->n_flag; ++i) {
            CDB2FLAG *f = (CDB2FLAG *)malloc(sizeof(CDB2FLAG));
            cdb2__flag__init(f);
            f->option = flags[i].getOption();
            f->value  = flags[i].getValue();
            sq->flag[i] = f;
        }
    }

    /* bind variables */
    auto bvs = r.getBindvars();
    sq->n_bindvars = bvs.size();
    if (sq->n_bindvars) {
        sq->bindvars = (CDB2SQLQUERY__Bindvalue **)malloc(
            sq->n_bindvars * sizeof(CDB2SQLQUERY__Bindvalue *));
        for (uint32_t i = 0; i < sq->n_bindvars; ++i) {
            auto bv = bvs[i];
            CDB2SQLQUERY__Bindvalue *b = (CDB2SQLQUERY__Bindvalue *)malloc(sizeof(CDB2SQLQUERY__Bindvalue));
            cdb2__sqlquery__bindvalue__init(b);
            b->varname = xstrdup(bv.getVarname().cStr());
            b->type    = bv.getType();
            auto val = bv.getValue();
            b->value.len  = val.size();
            b->value.data = xmemdup(val.begin(), val.size());
            b->has_isnull = 1;
            b->isnull     = bv.getIsnull();
            int32_t idx = bv.getIndex();
            if (idx != -1) {
                b->has_index = 1;
                b->index     = idx;
            }
            if (bv.hasCarray())
                b->carray = unpack_bind_array(bv.getCarray());
            sq->bindvars[i] = b;
        }
    }

    /* tzname */
    auto tz = r.getTzname();
    if (tz.size()) sq->tzname = xstrdup(tz.cStr());

    /* set_flags */
    auto sf = r.getSetFlags();
    sq->n_set_flags = sf.size();
    if (sq->n_set_flags) {
        sq->set_flags = (char **)malloc(sq->n_set_flags * sizeof(char *));
        for (uint32_t i = 0; i < sq->n_set_flags; ++i)
            sq->set_flags[i] = xstrdup(sf[i].cStr());
    }

    /* types */
    auto types = r.getTypes();
    sq->n_types = types.size();
    if (sq->n_types) {
        sq->types = (int32_t *)malloc(sq->n_types * sizeof(int32_t));
        for (uint32_t i = 0; i < sq->n_types; ++i)
            sq->types[i] = types[i];
    }

    /* mach_class */
    auto mc = r.getMachClass();
    sq->mach_class = xstrdup(mc.size() ? mc.cStr() : "unknown");

    /* cnonce */
    auto cnonce = r.getCnonce();
    if (cnonce.size()) {
        sq->has_cnonce = 1;
        sq->cnonce.len  = cnonce.size();
        sq->cnonce.data = xmemdup(cnonce.begin(), cnonce.size());
    }

    /* snapshot_info */
    if (r.hasSnapshotInfo())
        sq->snapshot_info = unpack_snapshotinfo_q(r.getSnapshotInfo());

    /* skip_rows */
    int64_t skip = r.getSkipRows();
    if (skip) {
        sq->has_skip_rows = 1;
        sq->skip_rows     = skip;
    }

    /* retry */
    int32_t retry = r.getRetry();
    sq->has_retry = 1;
    sq->retry     = retry;

    /* features */
    auto feats = r.getFeatures();
    sq->n_features = feats.size();
    if (sq->n_features) {
        sq->features = (int32_t *)malloc(sq->n_features * sizeof(int32_t));
        for (uint32_t i = 0; i < sq->n_features; ++i)
            sq->features[i] = feats[i];
    }

    /* client_info */
    if (r.hasClientInfo())
        sq->client_info = unpack_cinfo(r.getClientInfo());

    /* context */
    auto ctx = r.getContext();
    sq->n_context = ctx.size();
    if (sq->n_context) {
        sq->context = (char **)malloc(sq->n_context * sizeof(char *));
        for (uint32_t i = 0; i < sq->n_context; ++i)
            sq->context[i] = xstrdup(ctx[i].cStr());
    }

    /* req_info */
    if (r.hasReqInfo())
        sq->req_info = unpack_reqinfo(r.getReqInfo());

    /* identity */
    if (r.hasIdentity())
        sq->identity = unpack_identity(r.getIdentity());

    /* is_tagged */
    if (r.getIsTagged()) {
        sq->has_is_tagged = 1;
        sq->is_tagged     = 1;
    }

    return sq;
}

static CDB2DBINFO *
unpack_dbinfo(cdb2capnp::DbInfo::Reader r)
{
    CDB2DBINFO *di = (CDB2DBINFO *)malloc(sizeof(CDB2DBINFO));
    cdb2__dbinfo__init(di);
    di->dbname       = xstrdup(r.getDbname().cStr());
    di->little_endian = r.getLittleEndian();
    di->has_want_effects = 1;
    di->want_effects     = r.getWantEffects();
    return di;
}

static CDB2DISTTXN *
unpack_disttxn(cdb2capnp::DistTxn::Reader r)
{
    CDB2DISTTXN *dt = (CDB2DISTTXN *)malloc(sizeof(CDB2DISTTXN));
    cdb2__disttxn__init(dt);
    dt->dbname = xstrdup(r.getDbname().cStr());

    if (r.hasDisttxn()) {
        auto op = r.getDisttxn();
        CDB2DISTTXN__Disttxn *d = (CDB2DISTTXN__Disttxn *)malloc(sizeof(CDB2DISTTXN__Disttxn));
        cdb2__disttxn__disttxn__init(d);
        d->operation = op.getOperation();
        d->async     = op.getAsync();
        d->txnid     = xstrdup(op.getTxnid().cStr());
        d->name      = xstrdup(op.getName().cStr());
        d->tier      = xstrdup(op.getTier().cStr());
        d->master    = xstrdup(op.getMaster().cStr());
        d->has_rcode = 1; d->rcode = op.getRcode();
        d->has_outrc = 1; d->outrc = op.getOutrc();
        d->errmsg    = xstrdup(op.getErrmsg().cStr());
        dt->disttxn = d;
    }
    return dt;
}

/* ------------------------------------------------------------------ */
/* Pack helpers: protobuf-c structs → Cap'n Proto builders            */
/* ------------------------------------------------------------------ */

static void pack_snapshotinfo_resp(
    cdb2capnp::ResponseSnapshotInfo::Builder b,
    const CDB2SQLRESPONSE__Snapshotinfo *s)
{
    b.setFile(s->file);
    b.setOffset(s->offset);
}

static void pack_effects(cdb2capnp::Effects::Builder b,
                         const CDB2EFFECTS *e)
{
    b.setNumAffected(e->num_affected);
    b.setNumSelected(e->num_selected);
    b.setNumUpdated(e->num_updated);
    b.setNumDeleted(e->num_deleted);
    b.setNumInserted(e->num_inserted);
}

static void pack_nodeinfo(cdb2capnp::NodeInfo::Builder b,
                          const CDB2DBINFORESPONSE__Nodeinfo *n)
{
    if (n->name) b.setName(n->name);
    b.setNumber(n->number);
    b.setIncoherent(n->incoherent);
    if (n->has_room) b.setRoom(n->room);
    if (n->has_port) b.setPort(n->port);
}

static void pack_sqlquery(cdb2capnp::SqlQuery::Builder b,
                          const CDB2SQLQUERY *sq)
{
    if (sq->dbname)    b.setDbname(sq->dbname);
    if (sq->sql_query) b.setSqlQuery(sq->sql_query);
    b.setLittleEndian(sq->little_endian);

    if (sq->n_flag) {
        auto fl = b.initFlags(sq->n_flag);
        for (size_t i = 0; i < sq->n_flag; ++i) {
            fl[i].setOption(sq->flag[i]->option);
            fl[i].setValue(sq->flag[i]->value);
        }
    }

    if (sq->n_bindvars) {
        auto bvs = b.initBindvars(sq->n_bindvars);
        for (size_t i = 0; i < sq->n_bindvars; ++i) {
            const CDB2SQLQUERY__Bindvalue *bv = sq->bindvars[i];
            bvs[i].setVarname(bv->varname ? bv->varname : "");
            bvs[i].setType(bv->type);
            bvs[i].setValue(
                capnp::Data::Reader(bv->value.data, bv->value.len));
            if (bv->has_isnull) bvs[i].setIsnull(bv->isnull);
            bvs[i].setIndex(bv->has_index ? bv->index : -1);

            if (bv->carray) {
                auto ca = bvs[i].initCarray();
                const CDB2SQLQUERY__Bindvalue__Array *arr = bv->carray;
                switch (arr->type_case) {
                case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I32: {
                    auto a = ca.initI32();
                    auto e = a.initElements(arr->i32->n_elements);
                    for (size_t j = 0; j < arr->i32->n_elements; ++j)
                        e.set(j, arr->i32->elements[j]);
                    break;
                }
                case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_I64: {
                    auto a = ca.initI64();
                    auto e = a.initElements(arr->i64->n_elements);
                    for (size_t j = 0; j < arr->i64->n_elements; ++j)
                        e.set(j, arr->i64->elements[j]);
                    break;
                }
                case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_DBL: {
                    auto a = ca.initDbl();
                    auto e = a.initElements(arr->dbl->n_elements);
                    for (size_t j = 0; j < arr->dbl->n_elements; ++j)
                        e.set(j, arr->dbl->elements[j]);
                    break;
                }
                case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_TXT: {
                    auto a = ca.initTxt();
                    auto e = a.initElements(arr->txt->n_elements);
                    for (size_t j = 0; j < arr->txt->n_elements; ++j)
                        e.set(j, arr->txt->elements[j] ? arr->txt->elements[j] : "");
                    break;
                }
                case CDB2__SQLQUERY__BINDVALUE__ARRAY__TYPE_BLOB: {
                    auto a = ca.initBlob();
                    auto e = a.initElements(arr->blob->n_elements);
                    for (size_t j = 0; j < arr->blob->n_elements; ++j)
                        e.set(j, capnp::Data::Reader(
                            arr->blob->elements[j].data,
                            arr->blob->elements[j].len));
                    break;
                }
                default:
                    ca.setNone();
                    break;
                }
            }
        }
    }

    if (sq->tzname) b.setTzname(sq->tzname);

    if (sq->n_set_flags) {
        auto sf = b.initSetFlags(sq->n_set_flags);
        for (size_t i = 0; i < sq->n_set_flags; ++i)
            sf.set(i, sq->set_flags[i] ? sq->set_flags[i] : "");
    }

    if (sq->n_types) {
        auto types = b.initTypes(sq->n_types);
        for (size_t i = 0; i < sq->n_types; ++i)
            types.set(i, sq->types[i]);
    }

    if (sq->mach_class) b.setMachClass(sq->mach_class);

    if (sq->has_cnonce)
        b.setCnonce(capnp::Data::Reader(sq->cnonce.data, sq->cnonce.len));

    if (sq->snapshot_info) {
        auto si = b.initSnapshotInfo();
        si.setFile(sq->snapshot_info->file);
        si.setOffset(sq->snapshot_info->offset);
    }

    if (sq->has_skip_rows) b.setSkipRows(sq->skip_rows);
    if (sq->has_retry)     b.setRetry(sq->retry);

    if (sq->n_features) {
        auto feats = b.initFeatures(sq->n_features);
        for (size_t i = 0; i < sq->n_features; ++i)
            feats.set(i, sq->features[i]);
    }

    if (sq->client_info) {
        const CDB2SQLQUERY__Cinfo *ci = sq->client_info;
        auto cb = b.initClientInfo();
        cb.setPid(ci->pid);
        cb.setThId(ci->th_id);
        cb.setHostId(ci->host_id);
        if (ci->argv0)           cb.setArgv0(ci->argv0);
        if (ci->stack)           cb.setStack(ci->stack);
        if (ci->api_driver_name) cb.setApiDriverName(ci->api_driver_name);
        if (ci->api_driver_version) cb.setApiDriverVersion(ci->api_driver_version);
    }

    if (sq->n_context) {
        auto ctx = b.initContext(sq->n_context);
        for (size_t i = 0; i < sq->n_context; ++i)
            ctx.set(i, sq->context[i] ? sq->context[i] : "");
    }

    if (sq->req_info) {
        auto ri = b.initReqInfo();
        ri.setTimestampus(sq->req_info->timestampus);
        ri.setNumRetries(sq->req_info->num_retries);
    }

    if (sq->identity) {
        const CDB2SQLQUERY__IdentityBlob *id = sq->identity;
        auto ib = b.initIdentity();
        if (id->principal) ib.setPrincipal(id->principal);
        ib.setMajorVersion(id->majorversion);
        ib.setMinorVersion(id->minorversion);
        ib.setData(capnp::Data::Reader(id->data.data, id->data.len));
    }

    if (sq->has_is_tagged) b.setIsTagged(sq->is_tagged);
}

/* ------------------------------------------------------------------ */
/* Public API: unpack                                                  */
/* ------------------------------------------------------------------ */

extern "C" CDB2QUERY *
capnp_unpack_query(const uint8_t *buf, size_t len)
{
    if (!buf || !len) return nullptr;

    size_t nwords = (len + sizeof(capnp::word) - 1) / sizeof(capnp::word);
    kj::Array<capnp::word> aligned = kj::heapArray<capnp::word>(nwords);
    memcpy(aligned.begin(), buf, len);

    capnp::FlatArrayMessageReader reader(aligned.asPtr());

    auto q = reader.getRoot<cdb2capnp::Query>();

    CDB2QUERY *out = (CDB2QUERY *)malloc(sizeof(CDB2QUERY));
    cdb2__query__init(out);

    switch (q.which()) {
    case cdb2capnp::Query::SQLQUERY:
        if (q.hasSqlquery())
            out->sqlquery = unpack_sqlquery(q.getSqlquery());
        break;
    case cdb2capnp::Query::DBINFO:
        if (q.hasDbinfo())
            out->dbinfo = unpack_dbinfo(q.getDbinfo());
        break;
    case cdb2capnp::Query::SPCMD:
        out->spcmd = xstrdup(q.getSpcmd().cStr());
        break;
    case cdb2capnp::Query::DISTTXN:
        if (q.hasDisttxn())
            out->disttxn = unpack_disttxn(q.getDisttxn());
        break;
    default:
        break;
    }

    return out;
}

extern "C" void
capnp_free_query(CDB2QUERY *query)
{
    if (!query) return;
    /* All fields were malloc'd, so the system-allocator (NULL) free path
       in protobuf-c correctly frees everything. */
    cdb2__query__free_unpacked(query, nullptr);
}

extern "C" CDB2SQLRESPONSE *
capnp_unpack_sqlresponse(const uint8_t *buf, size_t len)
{
    if (!buf || !len) return nullptr;

    size_t nwords = (len + sizeof(capnp::word) - 1) / sizeof(capnp::word);
    kj::Array<capnp::word> aligned = kj::heapArray<capnp::word>(nwords);
    memcpy(aligned.begin(), buf, len);

    capnp::FlatArrayMessageReader reader(aligned.asPtr());
    auto r = reader.getRoot<cdb2capnp::SqlResponse>();

    CDB2SQLRESPONSE *out = (CDB2SQLRESPONSE *)malloc(sizeof(CDB2SQLRESPONSE));
    cdb2__sqlresponse__init(out);

    out->response_type = (ResponseType)from_capnp_rt(r.getResponseType());
    out->error_code    = (CDB2ErrorCode)r.getErrorCode();

    auto errstr = r.getErrorString();
    if (errstr.size()) out->error_string = xstrdup(errstr.cStr());

    /* nested column values */
    auto vals = r.getValue();
    out->n_value = vals.size();
    if (out->n_value) {
        out->value = (CDB2SQLRESPONSE__Column **)malloc(
            out->n_value * sizeof(CDB2SQLRESPONSE__Column *));
        for (uint32_t i = 0; i < out->n_value; ++i) {
            auto col = vals[i];
            CDB2SQLRESPONSE__Column *c = (CDB2SQLRESPONSE__Column *)malloc(sizeof(CDB2SQLRESPONSE__Column));
            cdb2__sqlresponse__column__init(c);
            c->has_type = 1;
            c->type     = (CDB2ColumnType)col.getType();
            auto v = col.getValue();
            c->value.len  = v.size();
            c->value.data = xmemdup(v.begin(), v.size());
            c->has_isnull = 1;
            c->isnull     = col.getIsnull();
            out->value[i] = c;
        }
    }

    if (r.hasEffects()) {
        auto ef = r.getEffects();
        CDB2EFFECTS *e = (CDB2EFFECTS *)malloc(sizeof(CDB2EFFECTS));
        cdb2__effects__init(e);
        e->num_affected = ef.getNumAffected();
        e->num_selected = ef.getNumSelected();
        e->num_updated  = ef.getNumUpdated();
        e->num_deleted  = ef.getNumDeleted();
        e->num_inserted = ef.getNumInserted();
        out->effects = e;
    }

    if (r.hasSnapshotInfo()) {
        auto si = r.getSnapshotInfo();
        CDB2SQLRESPONSE__Snapshotinfo *s = (CDB2SQLRESPONSE__Snapshotinfo *)malloc(sizeof(CDB2SQLRESPONSE__Snapshotinfo));
        cdb2__sqlresponse__snapshotinfo__init(s);
        s->file   = si.getFile();
        s->offset = si.getOffset();
        out->snapshot_info = s;
    }

    uint64_t rid = r.getRowId();
    if (rid) { out->has_row_id = 1; out->row_id = rid; }

    auto feats = r.getFeatures();
    out->n_features = feats.size();
    if (out->n_features) {
        out->features = (CDB2ServerFeatures *)malloc(
            out->n_features * sizeof(CDB2ServerFeatures));
        for (uint32_t i = 0; i < out->n_features; ++i)
            out->features[i] = (CDB2ServerFeatures)feats[i];
    }

    auto info = r.getInfoString();
    if (info.size()) out->info_string = xstrdup(info.cStr());

    if (r.getFlatColVals()) {
        out->has_flat_col_vals = 1;
        out->flat_col_vals     = 1;
    }

    auto fvs = r.getValues();
    out->n_values = fvs.size();
    if (out->n_values) {
        out->values = (ProtobufCBinaryData *)malloc(
            out->n_values * sizeof(ProtobufCBinaryData));
        for (uint32_t i = 0; i < out->n_values; ++i) {
            auto d = fvs[i];
            out->values[i].len  = d.size();
            out->values[i].data = xmemdup(d.begin(), d.size());
        }
    }

    auto nulls = r.getIsnulls();
    out->n_isnulls = nulls.size();
    if (out->n_isnulls) {
        out->isnulls = (protobuf_c_boolean *)malloc(
            out->n_isnulls * sizeof(protobuf_c_boolean));
        for (uint32_t i = 0; i < out->n_isnulls; ++i)
            out->isnulls[i] = nulls[i] ? 1 : 0;
    }

    auto fp = r.getFp();
    if (fp.size()) {
        out->has_fp     = 1;
        out->fp.len     = fp.size();
        out->fp.data    = xmemdup(fp.begin(), fp.size());
    }

    auto sr = r.getSqliteRow();
    if (sr.size()) {
        out->has_sqlite_row   = 1;
        out->sqlite_row.len   = sr.size();
        out->sqlite_row.data  = xmemdup(sr.begin(), sr.size());
    }

    auto fdb = r.getForeignDb();
    if (fdb.size()) out->foreign_db = xstrdup(fdb.cStr());

    auto fcls = r.getForeignClass();
    if (fcls.size()) out->foreign_class = xstrdup(fcls.cStr());

    int32_t fpf = r.getForeignPolicyFlag();
    if (fpf) { out->has_foreign_policy_flag = 1; out->foreign_policy_flag = fpf; }

    if (r.hasDisttxnresponse()) {
        CDB2DISTTXNRESPONSE *dr = (CDB2DISTTXNRESPONSE *)malloc(sizeof(CDB2DISTTXNRESPONSE));
        cdb2__disttxnresponse__init(dr);
        dr->rcode = r.getDisttxnresponse().getRcode();
        out->disttxnresponse = dr;
    }

    int32_t tail = r.getSqlTailOffset();
    if (tail) { out->has_sql_tail_offset = 1; out->sql_tail_offset = tail; }

    return out;
}

extern "C" CDB2DBINFORESPONSE *
capnp_unpack_dbinforesponse(const uint8_t *buf, size_t len)
{
    if (!buf || !len) return nullptr;

    size_t nwords = (len + sizeof(capnp::word) - 1) / sizeof(capnp::word);
    kj::Array<capnp::word> aligned = kj::heapArray<capnp::word>(nwords);
    memcpy(aligned.begin(), buf, len);

    capnp::FlatArrayMessageReader reader(aligned.asPtr());
    auto r = reader.getRoot<cdb2capnp::DbInfoResponse>();

    CDB2DBINFORESPONSE *out = (CDB2DBINFORESPONSE *)malloc(sizeof(CDB2DBINFORESPONSE));
    cdb2__dbinforesponse__init(out);

    /* master */
    CDB2DBINFORESPONSE__Nodeinfo *master = (CDB2DBINFORESPONSE__Nodeinfo *)malloc(sizeof(CDB2DBINFORESPONSE__Nodeinfo));
    cdb2__dbinforesponse__nodeinfo__init(master);
    auto mr = r.getMaster();
    master->name       = xstrdup(mr.getName().cStr());
    master->number     = mr.getNumber();
    master->incoherent = mr.getIncoherent();
    master->has_room   = 1; master->room = mr.getRoom();
    master->has_port   = 1; master->port = mr.getPort();
    out->master = master;

    /* nodes */
    auto nodes = r.getNodes();
    out->n_nodes = nodes.size();
    if (out->n_nodes) {
        out->nodes = (CDB2DBINFORESPONSE__Nodeinfo **)malloc(
            out->n_nodes * sizeof(CDB2DBINFORESPONSE__Nodeinfo *));
        for (uint32_t i = 0; i < out->n_nodes; ++i) {
            CDB2DBINFORESPONSE__Nodeinfo *n = (CDB2DBINFORESPONSE__Nodeinfo *)malloc(sizeof(CDB2DBINFORESPONSE__Nodeinfo));
            cdb2__dbinforesponse__nodeinfo__init(n);
            auto nr = nodes[i];
            n->name       = xstrdup(nr.getName().cStr());
            n->number     = nr.getNumber();
            n->incoherent = nr.getIncoherent();
            n->has_room   = 1; n->room = nr.getRoom();
            n->has_port   = 1; n->port = nr.getPort();
            out->nodes[i] = n;
        }
    }

    if (r.getRequireSsl()) {
        out->has_require_ssl = 1;
        out->require_ssl     = 1;
    }

    int32_t sm = r.getSyncMode();
    if (sm) {
        out->has_sync_mode = 1;
        out->sync_mode     = (CDB2SyncMode)sm;
    }

    return out;
}

extern "C" CDB2DISTTXNRESPONSE *
capnp_unpack_disttxnresponse(const uint8_t *buf, size_t len)
{
    if (!buf || !len) return nullptr;

    size_t nwords = (len + sizeof(capnp::word) - 1) / sizeof(capnp::word);
    kj::Array<capnp::word> aligned = kj::heapArray<capnp::word>(nwords);
    memcpy(aligned.begin(), buf, len);

    capnp::FlatArrayMessageReader reader(aligned.asPtr());
    auto r = reader.getRoot<cdb2capnp::DistTxnResponse>();

    CDB2DISTTXNRESPONSE *out = (CDB2DISTTXNRESPONSE *)malloc(sizeof(CDB2DISTTXNRESPONSE));
    cdb2__disttxnresponse__init(out);
    out->rcode = r.getRcode();
    return out;
}

extern "C" void
capnp_free_response(void *resp)
{
    if (!resp) return;
    /* The ProtobufCMessage base at offset 0 tells free_unpacked which
       descriptor to use.  All fields were malloc'd so NULL allocator works. */
    protobuf_c_message_free_unpacked((ProtobufCMessage *)resp, nullptr);
}

/* ------------------------------------------------------------------ */
/* Public API: pack                                                    */
/* ------------------------------------------------------------------ */

extern "C" size_t
capnp_pack_query(const CDB2QUERY *query, uint8_t **outbuf)
{
    if (!query || !outbuf) return 0;

    capnp::MallocMessageBuilder builder;
    auto q = builder.initRoot<cdb2capnp::Query>();

    if (query->sqlquery) {
        pack_sqlquery(q.initSqlquery(), query->sqlquery);
    } else if (query->dbinfo) {
        const CDB2DBINFO *di = query->dbinfo;
        auto db = q.initDbinfo();
        if (di->dbname) db.setDbname(di->dbname);
        db.setLittleEndian(di->little_endian);
        if (di->has_want_effects) db.setWantEffects(di->want_effects);
    } else if (query->spcmd) {
        q.setSpcmd(query->spcmd);
    } else if (query->disttxn) {
        const CDB2DISTTXN *dt = query->disttxn;
        auto dtb = q.initDisttxn();
        if (dt->dbname) dtb.setDbname(dt->dbname);
        if (dt->disttxn) {
            auto opb = dtb.initDisttxn();
            const CDB2DISTTXN__Disttxn *op = dt->disttxn;
            opb.setOperation(op->operation);
            opb.setAsync(op->async);
            if (op->txnid)  opb.setTxnid(op->txnid);
            if (op->name)   opb.setName(op->name);
            if (op->tier)   opb.setTier(op->tier);
            if (op->master) opb.setMaster(op->master);
            if (op->has_rcode) opb.setRcode(op->rcode);
            if (op->has_outrc) opb.setOutrc(op->outrc);
            if (op->errmsg) opb.setErrmsg(op->errmsg);
        }
    } else {
        q.setNone();
    }

    return builder_to_bytes(builder, outbuf);
}

extern "C" size_t
capnp_pack_sqlresponse(const CDB2SQLRESPONSE *resp, uint8_t **outbuf)
{
    if (!resp || !outbuf) return 0;

    capnp::MallocMessageBuilder builder;
    auto r = builder.initRoot<cdb2capnp::SqlResponse>();

    r.setResponseType(to_capnp_rt(resp->response_type));
    r.setErrorCode(resp->error_code);

    if (resp->error_string) r.setErrorString(resp->error_string);

    if (resp->n_value) {
        auto cols = r.initValue(resp->n_value);
        for (size_t i = 0; i < resp->n_value; ++i) {
            const CDB2SQLRESPONSE__Column *c = resp->value[i];
            if (c->has_type) cols[i].setType(c->type);
            cols[i].setValue(
                capnp::Data::Reader(c->value.data, c->value.len));
            if (c->has_isnull) cols[i].setIsnull(c->isnull);
        }
    }

    if (resp->effects) pack_effects(r.initEffects(), resp->effects);

    if (resp->snapshot_info)
        pack_snapshotinfo_resp(r.initSnapshotInfo(), resp->snapshot_info);

    if (resp->has_row_id) r.setRowId(resp->row_id);

    if (resp->n_features) {
        auto feats = r.initFeatures(resp->n_features);
        for (size_t i = 0; i < resp->n_features; ++i)
            feats.set(i, (int32_t)resp->features[i]);
    }

    if (resp->info_string) r.setInfoString(resp->info_string);

    if (resp->has_flat_col_vals) r.setFlatColVals(resp->flat_col_vals);

    if (resp->n_values) {
        auto fvs = r.initValues(resp->n_values);
        for (size_t i = 0; i < resp->n_values; ++i)
            fvs.set(i, capnp::Data::Reader(resp->values[i].data,
                                           resp->values[i].len));
    }

    if (resp->n_isnulls) {
        auto nulls = r.initIsnulls(resp->n_isnulls);
        for (size_t i = 0; i < resp->n_isnulls; ++i)
            nulls.set(i, resp->isnulls[i] != 0);
    }

    if (resp->has_fp)
        r.setFp(capnp::Data::Reader(resp->fp.data, resp->fp.len));

    if (resp->has_sqlite_row)
        r.setSqliteRow(
            capnp::Data::Reader(resp->sqlite_row.data, resp->sqlite_row.len));

    if (resp->foreign_db)    r.setForeignDb(resp->foreign_db);
    if (resp->foreign_class) r.setForeignClass(resp->foreign_class);
    if (resp->has_foreign_policy_flag)
        r.setForeignPolicyFlag(resp->foreign_policy_flag);

    if (resp->disttxnresponse)
        r.initDisttxnresponse().setRcode(resp->disttxnresponse->rcode);

    if (resp->has_sql_tail_offset)
        r.setSqlTailOffset(resp->sql_tail_offset);

    return builder_to_bytes(builder, outbuf);
}

extern "C" size_t
capnp_pack_dbinforesponse(const CDB2DBINFORESPONSE *resp, uint8_t **outbuf)
{
    if (!resp || !outbuf) return 0;

    capnp::MallocMessageBuilder builder;
    auto r = builder.initRoot<cdb2capnp::DbInfoResponse>();

    if (resp->master) pack_nodeinfo(r.initMaster(), resp->master);

    if (resp->n_nodes) {
        auto nodes = r.initNodes(resp->n_nodes);
        for (size_t i = 0; i < resp->n_nodes; ++i)
            pack_nodeinfo(nodes[i], resp->nodes[i]);
    }

    if (resp->has_require_ssl) r.setRequireSsl(resp->require_ssl);
    if (resp->has_sync_mode)   r.setSyncMode((int32_t)resp->sync_mode);

    return builder_to_bytes(builder, outbuf);
}

extern "C" size_t
capnp_pack_disttxnresponse(const CDB2DISTTXNRESPONSE *resp, uint8_t **outbuf)
{
    if (!resp || !outbuf) return 0;

    capnp::MallocMessageBuilder builder;
    builder.initRoot<cdb2capnp::DistTxnResponse>().setRcode(resp->rcode);
    return builder_to_bytes(builder, outbuf);
}
