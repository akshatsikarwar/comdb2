#pragma once

/*
 * Cap'n Proto codec: extern "C" API for converting between the Cap'n Proto
 * wire format and the protobuf-c C struct types used internally.
 *
 * The server uses capnp_unpack_query / capnp_pack_* when a client connects
 * with newsqlheader.type == CAPNP_CDB2QUERY.  The client uses capnp_pack_query
 * / capnp_unpack_* when cdb2_use_capnproto is set.
 *
 * All returned structs are malloc'd and must be freed with the corresponding
 * capnp_free_* function, NOT with the protobuf-c free_unpacked routines.
 */

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward-declare the protobuf-c generated types used as the internal
   representation.  The actual definitions are in sqlquery.pb-c.h /
   sqlresponse.pb-c.h which must be included by the caller before this
   header when the full struct layout is needed. */
struct CDB2QUERY;
struct CDB2DBINFO;
struct CDB2DISTTXN;
struct CDB2SQLQUERY;
struct CDB2SQLRESPONSE;
struct CDB2DBINFORESPONSE;
struct CDB2DISTTXNRESPONSE;

/* newsqlheader.type value for Cap'n Proto queries (server side) */
#define CAPNP_CDB2QUERY 3

/* ------------------------------------------------------------------ */
/* Server-side: decode incoming Cap'n Proto query, encode responses    */
/* ------------------------------------------------------------------ */

/*
 * Parse a Cap'n Proto-encoded CDB2_QUERY from the wire.
 * Returns a malloc-populated CDB2QUERY* (same struct layout as the
 * protobuf-c generated type) on success, or NULL on parse error.
 * Free with capnp_free_query().
 */
struct CDB2QUERY *capnp_unpack_query(const uint8_t *buf, size_t len);

/* Free a CDB2QUERY* returned by capnp_unpack_query(). */
void capnp_free_query(struct CDB2QUERY *query);

/*
 * Serialize a CDB2SQLRESPONSE to Cap'n Proto wire format.
 * *outbuf is set to a malloc'd buffer owned by the caller (free with free()).
 * Returns the number of bytes written, or 0 on error.
 */
size_t capnp_pack_sqlresponse(const struct CDB2SQLRESPONSE *resp,
                              uint8_t **outbuf);

/*
 * Serialize a CDB2DBINFORESPONSE to Cap'n Proto wire format.
 */
size_t capnp_pack_dbinforesponse(const struct CDB2DBINFORESPONSE *resp,
                                 uint8_t **outbuf);

/*
 * Serialize a CDB2DISTTXNRESPONSE to Cap'n Proto wire format.
 */
size_t capnp_pack_disttxnresponse(const struct CDB2DISTTXNRESPONSE *resp,
                                  uint8_t **outbuf);

/* ------------------------------------------------------------------ */
/* Client-side: encode outgoing queries, decode incoming responses     */
/* ------------------------------------------------------------------ */

/*
 * Serialize a CDB2QUERY to Cap'n Proto wire format.
 * *outbuf is set to a malloc'd buffer owned by the caller (free with free()).
 * Returns the number of bytes written, or 0 on error.
 */
size_t capnp_pack_query(const struct CDB2QUERY *query, uint8_t **outbuf);

/*
 * Parse a Cap'n Proto-encoded CDB2SQLRESPONSE from the wire.
 * Returns a malloc-populated CDB2SQLRESPONSE* or NULL on error.
 * Free with capnp_free_response().
 */
struct CDB2SQLRESPONSE *capnp_unpack_sqlresponse(const uint8_t *buf,
                                                 size_t len);

/*
 * Parse a Cap'n Proto-encoded CDB2DBINFORESPONSE from the wire.
 */
struct CDB2DBINFORESPONSE *capnp_unpack_dbinforesponse(const uint8_t *buf,
                                                       size_t len);

/*
 * Parse a Cap'n Proto-encoded CDB2DISTTXNRESPONSE from the wire.
 */
struct CDB2DISTTXNRESPONSE *capnp_unpack_disttxnresponse(const uint8_t *buf,
                                                         size_t len);

/*
 * Free any struct returned by a capnp_unpack_* response function.
 * (The opaque type parameter accepts CDB2SQLRESPONSE*, CDB2DBINFORESPONSE*,
 *  or CDB2DISTTXNRESPONSE*.)
 */
void capnp_free_response(void *resp);

#ifdef __cplusplus
}
#endif
