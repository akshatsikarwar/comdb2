#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#include <stdlib.h>
#include <string.h>

#include <string>
#include <sstream>
#include <iostream>

#include <sqlite3.h>
#include <cdb2api.h>
#include <cson_amalgamation_core.h>

static int run_stmt(cdb2_hndl_tp *db, const char *sql)
{
    if (strncmp(sql, "begin", 5) != 0 &&
        strncmp(sql, "commit", 6) != 0 &&
        strncmp(sql, "put", 3) != 0) {
        puts(sql);
    }

    int rc = cdb2_run_statement(db, sql);
    while (rc == CDB2_OK) {
        rc = cdb2_next_record(db);
    }
    if (rc == CDB2_OK_DONE) {
        return 0;
    }
    fprintf(stderr, "%s -- %s rc:%d err:%s\n", __func__, sql, rc,
            cdb2_errstr(db));
    exit(EXIT_FAILURE);
}

static void next_id(cdb2_hndl_tp *db, const char *proc, int64_t *next)
{
    std::string sql("get lua replicant ");
    sql += proc;
    int rc = cdb2_run_statement(db, sql.c_str());
    if ((rc = cdb2_next_record(db)) == CDB2_OK) {
        *next = *(int64_t *)cdb2_column_value(db, 0);
        while (cdb2_next_record(db) == CDB2_OK)
            ;
    } else {
        fprintf(stderr, "%s -- %s rc:%d err:%s\n", __func__, sql.c_str(), rc,
                cdb2_errstr(db));
        exit(EXIT_FAILURE);
    }
}

static int usage(int exit_val)
{
    fprintf(
        exit_val == EXIT_SUCCESS ? stdout : stderr,
        "usage: cdb2rcv <-s dbname> <-d dbname> "
#ifdef SQLITE_BKUP
        "<-c cache-location> "
#endif
        "<procedure>\n"
        "  -s dbname\t\tsource database name[@machine0,machine1]\n"
        "  -d dbname\t\tdestination database name[@machine0,machine1]\n"
#ifdef SQLITE_BKUP
        "  -c cache-localtion\tlocation to store temp cache for re-trans\n"
#endif
        );
    exit(exit_val);
}

static cdb2_hndl_tp *db_connect(char *name)
{
    if (name == NULL) return NULL;
    char *cluster = NULL;
    char *x = strchr(name, '@');
    if (x == NULL) {
        cluster = strdup("default");
    } else {
        cluster = strdup(x);
        *x = 0;
    }
    cdb2_hndl_tp *hndl = NULL;
    cdb2_open(&hndl, name, cluster, 0);
    free(cluster);
    return hndl;
}

static uint64_t get_genid(cson_object *o)
{
    cson_value *v = cson_object_get(o, "genid");
    const char *genid = cson_value_get_cstr(v);
    return strtoull(genid, NULL, 16);
}

/* returns id of event processed */
static int64_t process_event(cdb2_hndl_tp *dest, char *proc, const char *json,
                             int len)
{
    cson_value *v0, *v;
    cson_parse_string(&v0, json, len, NULL, NULL);
    cson_object *o = cson_value_get_object(v0);

    v = cson_object_get(o, "id");
    int64_t id = cson_value_get_integer(v);

    v = cson_object_get(o, "event");
    cson_array *event = cson_value_get_array(v);
    unsigned n = cson_array_length_get(event);

    run_stmt(dest, "begin");

    for (unsigned i = 0; i < n; ++i) {
        v = cson_array_get(event, i);
        o = cson_value_get_object(v);

        v = cson_object_get(o, "tbl");
        const char *tbl = cson_value_get_cstr(v);

        v = cson_object_get(o, "type");
        const char *type = cson_value_get_cstr(v);

        if (strcmp(type, "ddl") == 0) {
            if (n != 1) abort();
            v = cson_object_get(o, "ddl");
            run_stmt(dest, cson_value_get_cstr(v));
        } else if (strcmp(type, "del") == 0) {
            uint64_t genid = get_genid(o);
            std::stringstream sql;
            sql << "delete from " << tbl << " where comdb2_rowid='2:" << genid
                << "'";
            run_stmt(dest, sql.str().c_str());
        } else if (strcmp(type, "add") == 0) {
            v = cson_object_get(o, "genid");
            const char *genid = cson_value_get_cstr(v);

            v = cson_object_get(o, "cols");
            cson_array *cols = cson_value_get_array(v);

            const char *comma = "";
            std::stringstream ss_cols, ss_values;
            unsigned ncols = cson_array_length_get(cols);
            for (unsigned j = 0; j < ncols; ++j, comma = ",") {
                cson_value *v = cson_array_get(cols, j);
                cson_object *col = cson_value_get_object(v);

                v = cson_object_get(col, "name");
                ss_cols << comma << '"' << cson_value_get_cstr(v) << '"';

                v = cson_object_get(col, "type");
                const char *ctype = cson_value_get_cstr(v);

                v = cson_object_get(col, "value");
                if (cson_value_is_null(v)) {
                    ss_values << comma << "null";
                } else if (cson_value_is_integer(v)) {
                    ss_values << comma << cson_value_get_integer(v);
                } else if (cson_value_is_double(v)) {
                    ss_values << comma << cson_value_get_double(v);
                } else if (cson_value_is_string(v)) {
                    if (strcmp(ctype, "blob") == 0 ||
                        strcmp(ctype, "bytearray") == 0) {
                        ss_values << comma << "x'" << cson_value_get_cstr(v)
                                  << "'";
                    } else {
                        char *qstr =
                            sqlite3_mprintf("%Q", cson_value_get_cstr(v));
                        ss_values << comma << qstr;
                        sqlite3_free(qstr);
                    }
                }
            }
            std::stringstream sql;
            sql << "insertgenid x'" << genid << "' into \"" << tbl << "\"("
                << ss_cols.str() << ") values(" << ss_values.str() << ")";
            run_stmt(dest, sql.str().c_str());
        } else if (strcmp(type, "upd") == 0) {
            uint64_t genid = get_genid(o);

            v = cson_object_get(o, "cols");
            cson_array *cols = cson_value_get_array(v);

            const char *comma = "";
            std::stringstream ss_upds;
            unsigned ncols = cson_array_length_get(cols);
            for (unsigned j = 0; j < ncols; ++j, comma = ",") {
                cson_value *v = cson_array_get(cols, j);
                cson_object *col = cson_value_get_object(v);

                v = cson_object_get(col, "name");
                ss_upds << comma << '"' << cson_value_get_cstr(v) << "\"=";

                v = cson_object_get(col, "type");
                const char *ctype = cson_value_get_cstr(v);

                v = cson_object_get(col, "value");
                if (cson_value_is_null(v)) {
                    ss_upds << "null";
                } else if (cson_value_is_integer(v)) {
                    ss_upds << cson_value_get_integer(v);
                } else if (cson_value_is_double(v)) {
                    ss_upds << cson_value_get_double(v);
                } else if (cson_value_is_string(v)) {
                    if (strcmp(ctype, "blob") == 0 ||
                        strcmp(ctype, "bytearray") == 0) {
                        ss_upds << "x'" << cson_value_get_cstr(v) << "'";
                    } else {
                        char *qstr =
                            sqlite3_mprintf("%Q", cson_value_get_cstr(v));
                        ss_upds << qstr;
                        sqlite3_free(qstr);
                    }
                }
            }
            std::stringstream sql;
            sql << "update " << tbl << " set " << ss_upds.str()
                << " where comdb2_rowid='2:" << genid << "'";
            run_stmt(dest, sql.str().c_str());
        }
    }
    // save next expected event id
    std::stringstream idsql;
    idsql << "put lua replicant " << proc << " " << id + 1;
    run_stmt(dest, idsql.str().c_str());

    run_stmt(dest, "commit");

    cson_value_free(v0);
    return id;
}

#ifdef SQLITE_BKUP
typedef struct {
    cdb2_hndl_tp *dest;
    char *proc;
    int64_t id;
} callback_t;

static int callback(void *varg, int argc, char **argv, char **azColName)
{
    callback_t *arg = (callback_t *)varg;
    int64_t id = strtoll(argv[0], NULL, 10);
    if (id != arg->id) {
        fprintf(stderr, "missing data in retrans cache have:%" PRId64
                        " need:%" PRId64 "\n",
                id, arg->id);
        return -1;
    }
    arg->id = process_event(arg->dest, arg->proc, argv[1], strlen(argv[1]));
    ++arg->id;
    return 0;
}
#endif

static int64_t catch_up(cdb2_hndl_tp *dest, char *proc, sqlite3 *cache,
                    int64_t from, int64_t to)
{
#ifdef SQLITE_BKUP
    char *foo =
        sqlite3_mprintf("select id, event from events where id between %" PRId64
                        " and %" PRId64 ";",
                        from, to - 1);
    printf("%s -- %s\n", __func__, foo);
    callback_t arg = {.dest = dest, .proc = proc, .id = from};
    if (sqlite3_exec(cache, foo, callback, &arg, 0) != 0) {
        sqlite3_close(cache);
        exit(EXIT_FAILURE);
    }
    sqlite3_free(foo);
    printf("done catching up best i could from:%ld to:%ld target-to:%ld\n",
           from, arg.id, to);
    return arg.id;
#else
    fprintf(stderr, "not compiled with local cache\n");
    return -1;
#endif
}

int main(int argc, char **argv)
{
    char *s = NULL, *d = NULL, *c = NULL;
    argc--;
    argv++;
    while (argc && argv[0][0] == '-') {
        if (strcmp(argv[0], "-h") == 0) {
            return usage(EXIT_SUCCESS);
        } else if (strcmp(argv[0], "-s") == 0) {
            argc--;
            argv++;
            s = argv[0];
        } else if (strcmp(argv[0], "-d") == 0) {
            argc--;
            argv++;
            d = argv[0];
#ifdef SQLITE_BKUP
        } else if (strcmp(argv[0], "-c") == 0) {
            argc--;
            argv++;
            c = argv[0];
#endif
        }
        argc--;
        argv++;
    }
    char *proc = argc == 1 ? argv[0] : NULL;
    if (proc == NULL) {
        fprintf(stderr, "missing procedure name\n");
        return usage(EXIT_FAILURE);
    }
#ifdef SQLITE_BKUP
    if (c == NULL) {
        fprintf(stderr, "missing cache-location\n");
        return usage(EXIT_FAILURE);
    }
#endif
    cdb2_hndl_tp *src = db_connect(s);
    cdb2_hndl_tp *dest = db_connect(d);

    sqlite3 *cache = NULL;
#ifdef SQLITE_BKUP
    std::string cache_path(c);
    if (cache_path.back() != '/')
        cache_path += '/';
    cache_path += d;
    cache_path += ".cache";
    sqlite3_open(cache_path.c_str(), &cache);
    const char *create = "create table if not exists events(id integer primary "
                         "key, event text) without rowid";
    sqlite3_exec(cache, create, NULL, NULL, NULL);
#endif

    if (src == NULL || dest == NULL) {
        fprintf(stderr, "failed to connect src:%p dest:%p\n", src, dest);
        return EXIT_FAILURE;
    }

    int64_t next;
    next_id(dest, proc, &next);

    std::stringstream ss;
    ss << "exec procedure " << proc << "()";
    int rc = cdb2_run_statement(src, ss.str().c_str());
    if (rc != CDB2_OK) {
        return EXIT_FAILURE;
    }
    while ((rc = cdb2_next_record(src)) == CDB2_OK) {
        char *json = (char *)cdb2_column_value(src, 0);
        int len = cdb2_column_size(src, 0) - 1;

        cson_value *v;
        cson_parse_string(&v, json, len, NULL, NULL);
        cson_object *o = cson_value_get_object(v);
        v = cson_object_get(o, "id");
        int64_t id = cson_value_get_integer(v);
        cson_value_free(cson_object_value(o));
        if (id > next) {
            fprintf(stderr,
                    "replicant out of sync id:%" PRId64 " next:%" PRId64 "\n",
                    id, next);
            fprintf(stderr, "catching up from local cache\n");
            if ((next = catch_up(dest, proc, cache, next, id)) != id) {
                fprintf(stderr,
                        "hole in retrans cache -- hot-copy from source");
                exit(EXIT_FAILURE);
            }
        }
        if (id == next) break;
    }

    do {
        char *json = (char *)cdb2_column_value(src, 0);
        int len = cdb2_column_size(src, 0) - 1;
        int64_t id = process_event(dest, proc, json, len);
#ifdef SQLITE_BKUP
        char *foo;
        foo = sqlite3_mprintf(
            "insert or replace into events(id, event) values(%" PRId64 ", %Q);",
            id, json);
        sqlite3_exec(cache, foo, NULL, NULL, NULL);
        sqlite3_free(foo);

        const int64_t history = 1000;
        if (id % history == 0) {
            int64_t del = id - history;
            foo = sqlite3_mprintf("delete from events where id < %" PRId64 ";",
                                  del);
            sqlite3_exec(cache, foo, NULL, NULL, NULL);
            sqlite3_free(foo);
        }
#endif
    } while ((rc = cdb2_next_record(src)) == CDB2_OK);
}
