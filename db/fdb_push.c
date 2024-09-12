/*
   Copyright 2200 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#include "sql.h"
#include "dohsql.h"

extern char *gbl_cdb2api_policy_override;
extern int gbl_fdb_auth_enabled;

struct fdb_push_connector {
    char *remotedb; /* name of the remote db; class matches stored fdb */
    enum mach_class class; /* what stage this db lives on */
    int class_override; /* class was explicit in the remdb name */
    int local;      /* is this a local db */
    int ncols;
    int rowlen;  /* current row len returned from remote */
    void *row;   /* current row returned from remote */
    Mem *unprow; /* generated by unpacking the row */
    int nparams; /* number of bound parameters */
    struct param_data *params; /* bound parameters */
};

static void _master_clnt_set(struct sqlclntstate *clnt);
static int set_bound_parameters(fdb_push_connector_t *push, cdb2_hndl_tp *hndl,
                                const char *tzname, struct errstat *err,
                                cdb2_client_datetime_t *dts);

static int convert_policy_override_string_to_cdb2api_flag(char *policy) {
    if (policy == NULL) {
        return 0;
    }

    if (strcmp(policy, "CDB2_RANDOM") == 0) {
        return CDB2_RANDOM;
    } else if (strcmp(policy, "CDB2_RANDOMROOM") == 0) {
        return CDB2_RANDOMROOM;
    } else if (strcmp(policy, "CDB2_ROOM") == 0) {
        return CDB2_ROOM;
    } else {
        return 0;
    }
}

/**
 * Remote query push support
 * Save in clnt information that this is a standalone select that
 * refers to a remote table
 *
 */
int fdb_push_run(Parse *pParse, dohsql_node_t *node)
{
    GET_CLNT;
    fdb_push_connector_t *push = NULL;
    struct Db *pDb = &pParse->db->aDb[node->remotedb];

    logmsg(LOGMSG_DEBUG,
           "CALLING FDB PUSH RUN on query %s (gbl_fdb_push_remote=%d) (clnt force remote? %d) (clnt force redirect? "
           "%d) (clnt can redirect? %d)\n",
           clnt->sql, gbl_fdb_push_remote, clnt->force_fdb_push_remote, clnt->force_fdb_push_redirect,
           clnt->can_redirect_fdb);
    if (!gbl_fdb_push_remote && !clnt->force_fdb_push_remote && !clnt->force_fdb_push_redirect)
        return -1;

    if (clnt->disable_fdb_push)
        return -1;

    if (clnt->intrans || clnt->in_client_trans)
        return -1;

    if (pDb->version < FDB_VER_PROXY)
        return -1;

    push = calloc(1, sizeof(fdb_push_connector_t));
    if (!push) {
        logmsg(LOGMSG_ERROR, "Failed to allocate fdb_push\n");
        return -1;
    }
    push->remotedb = strdup(pDb->zDbSName);
    if (!push->remotedb) {
        logmsg(LOGMSG_ERROR, "Failed to allocate remotedb name\n");
        free(push);
        return -1;
    }
    push->class =  pDb->class;
    push->local = pDb->local;
    push->class_override = pDb->class_override;

    push->ncols = node->ncols;

    if (node->params) {
        if (dohsql_clone_params(node->params->nparams, node->params->params,
                                &push->nparams, &push->params)) {
            free(push->remotedb);
            free(push);
            return -1;
        }
    }

    _master_clnt_set(clnt);

    clnt->fdb_push = push;

    return 0;
}

/**
 * Free remote push support
 */
void fdb_push_free(fdb_push_connector_t **pp)
{
    fdb_push_connector_t *p = *pp;
    if (p) {
        if (p->nparams && p->params)
            dohsql_free_params(&p->nparams, &p->params,
                               p->nparams-1);
        free(p->remotedb);
        if (p->unprow) {
            sqlite3UnpackedResultFree(&p->unprow, p->ncols);
        }
        *pp = NULL;
    }
}

/* override sqlite engine */
static int fdb_push_column_count(struct sqlclntstate *clnt, sqlite3_stmt *_)
{
    return clnt->fdb_push->ncols;
}

/* Typecasting may reallocate zMalloc. So do it while holding master_mem_mtx. */
#define FUNC_COLUMN_TYPE(ret, type, err_ret)                                   \
    static ret fdb_push_column_##type(struct sqlclntstate *clnt,               \
                                      sqlite3_stmt *stmt, int iCol)            \
    {                                                                          \
        fdb_push_connector_t *p = clnt->fdb_push;                              \
        ret rv;                                                                \
        if (!p->unprow)                                                        \
            p->unprow = sqlite3UnpackedResult(stmt, p->ncols, p->row,          \
                                              p->rowlen);                      \
        if (!p->unprow)                                                        \
            return err_ret;                                                    \
        if (iCol < 0 || iCol >= p->ncols)                                      \
            return err_ret;                                                    \
        rv = sqlite3_value_##type(&p->unprow[iCol]);                           \
        return rv;                                                             \
    }

FUNC_COLUMN_TYPE(int, type, 0)
FUNC_COLUMN_TYPE(sqlite_int64, int64, 0)
FUNC_COLUMN_TYPE(double, double, 0)
FUNC_COLUMN_TYPE(int, bytes, 0)
FUNC_COLUMN_TYPE(const unsigned char *, text, NULL)
FUNC_COLUMN_TYPE(const void *, blob, NULL)
FUNC_COLUMN_TYPE(const dttz_t *, datetime, NULL)

static const intv_t *fdb_push_column_interval(struct sqlclntstate *clnt,
                                              sqlite3_stmt *stmt, int iCol,
                                              int type)
{
    fdb_push_connector_t *p = clnt->fdb_push;
    const intv_t *rv;
    if (!p->unprow)
        p->unprow = sqlite3UnpackedResult(stmt, p->ncols, p->row, p->rowlen);
    if (!p->unprow)
        return NULL;
    if (iCol < 0 || iCol >= p->ncols)
        return NULL;

    rv = sqlite3_value_interval(&p->unprow[iCol], type);
    return rv;
}

static char *_get_tzname(struct sqlclntstate *clnt, sqlite3_stmt *_)
{
    return clnt->tzname;
}

static void _master_clnt_set(struct sqlclntstate *clnt)
{
    clnt->backup = clnt->plugin;
    clnt->adapter_backup = clnt->adapter;

    clnt->plugin.column_count = fdb_push_column_count;
    clnt->plugin.column_type = fdb_push_column_type;
    clnt->plugin.column_int64 = fdb_push_column_int64;
    clnt->plugin.column_double = fdb_push_column_double;
    clnt->plugin.column_text = fdb_push_column_text;
    clnt->plugin.column_bytes = fdb_push_column_bytes;
    clnt->plugin.column_blob = fdb_push_column_blob;
    clnt->plugin.column_datetime = fdb_push_column_datetime;
    clnt->plugin.column_interval = fdb_push_column_interval;
    clnt->plugin.column_value = NULL; // TODO: implement this
    clnt->plugin.tzname = _get_tzname;
}

static int _get_remote_cost(struct sqlclntstate *clnt, cdb2_hndl_tp *hndl);
void *(*externalComdb2getAuthIdBlob)(void *ID) = NULL;

/**
 * Proxy receiving sqlite rows from remote and forwarding them to
 * client after conversion to comdb2 format
 *
 * Returns: 0 if ok
 *          -1 if unrecoverable error
 *          -2 if remote is too old (7.0 or older)
 */
int handle_fdb_push(struct sqlclntstate *clnt, struct errstat *err)
{
    fdb_push_connector_t *push = clnt->fdb_push;
    cdb2_hndl_tp *hndl = NULL;
    uint64_t row_id = 0;
    int first_row = 1;
    int rc, irc;

    const char *class = "default";
    int cdb2api_policy_flag = push->local ? 0 : convert_policy_override_string_to_cdb2api_flag(gbl_cdb2api_policy_override);
    if (push->local)
        class = "local";
    else if (push->class_override) {
        class = mach_class_class2name(push->class);
        assert(class);
    }

    if ((gbl_fdb_push_redirect_foreign || clnt->force_fdb_push_redirect) && clnt->can_redirect_fdb &&
        !clnt->force_fdb_push_remote) {
        logmsg(LOGMSG_DEBUG,
               "CALLING FDB PUSH REDIRECT on query %s (redirect tunable on %d) (clnt force remote? %d) (clnt force "
               "redirect? %d)\n",
               clnt->sql, gbl_fdb_push_redirect_foreign, clnt->force_fdb_push_remote, clnt->force_fdb_push_redirect);
        // tell cdb2api to run query directly on foreign db
        // send back db, tier, flag
        // NOTE: Cost will not work for this
        if (push->local) { // this is local to server, not client. Return hostname to client
            cdb2api_policy_flag |= CDB2_DIRECT_CPU;
            class = gbl_myhostname;
        }
        rc = 0;
        const char *foreign_db[2];
        foreign_db[0] = push->remotedb;
        foreign_db[1] = class;
        write_response(clnt, RESPONSE_REDIRECT_FOREIGN, foreign_db, cdb2api_policy_flag);
        goto reset;
    }
    logmsg(LOGMSG_DEBUG,
           "CALLING FDB PUSH on query %s (redirect tunable on? %d) (clnt force remote? %d) (clnt force redirect? %d) "
           "(clnt can redirect? %d)\n",
           clnt->sql, gbl_fdb_push_redirect_foreign, clnt->force_fdb_push_remote, clnt->force_fdb_push_redirect,
           clnt->can_redirect_fdb);

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    rc = cdb2_open(&hndl, push->remotedb, class, CDB2_SQL_ROWS | cdb2api_policy_flag);
    if (rc) {
        errstat_set_rcstrf(err, rc, "Failed to open db %s local", push->remotedb);
        return -1;
    }

    rc = forward_set_commands(clnt, hndl, err);
    if (rc) {
        return -1;
    }

    cdb2_client_datetime_t *dts = calloc(push->nparams, sizeof(*dts));
    rc = set_bound_parameters(push, hndl, clnt->tzname, err, dts);
    if (rc) {
        free(dts);
        return -1;
    }

    if (gbl_uses_externalauth && gbl_fdb_auth_enabled && externalComdb2getAuthIdBlob)
        cdb2_setIdentityBlob(hndl, externalComdb2getAuthIdBlob(clnt->authdata));

    rc = cdb2_run_statement(hndl, clnt->sql);
    free(dts);
    if (rc) {
        const char *errstr = cdb2_errstr(hndl);
        if (errstr &&
            strncasecmp("remote is R7 or lower", errstr, strlen(errstr)) == 0) {
            rc = -2;
            goto closing;
        }
        errstat_set_rcstrf(err, rc = -1, "Failed to run query", clnt->sql);
        goto send_error;
    }

    int ncols = cdb2_numcolumns(hndl);

    rc = cdb2_next_record(hndl);
    while (rc == CDB2_OK) {
        push->row = cdb2_column_value(hndl, 0);
        push->rowlen = cdb2_column_size(hndl, 0);

        if (first_row) {
            /* send column info */
            first_row = 0;
            irc = write_response(clnt, RESPONSE_COLUMNS_FDB_PUSH, hndl, ncols);
            if (irc) {
                errstat_set_rcstrf(err, rc = -1, "Failed to send columns");
                goto closing;
            }
        }

        /* send row */
        row_id++;
        irc = send_row(clnt, NULL, row_id, 0, err);
        if (irc) {
            logmsg(LOGMSG_ERROR, "Failed to send row id %llu",
                   (long long unsigned int)row_id);
            goto closing;
        }

        /* we are done with the sqlite row, free it here */
        if (push->unprow) {
            sqlite3UnpackedResultFree(&push->unprow, push->ncols);
            push->unprow = NULL;
        }


        irc = comdb2_sql_tick();
        if (irc) {
            errstat_set_rcstrf(err, rc = -1, "Sql tick failed");
            goto closing;
        }

        /* next row */
        rc = cdb2_next_record(hndl);
        if (rc != CDB2_OK && rc != CDB2_OK_DONE) {
            errstat_set_rcstrf(err, rc = -1, "Next record error %d", irc);
        }
    }
send_error:
    if (rc != CDB2_OK_DONE) {
        /* send error */
        logmsg(LOGMSG_ERROR, "Query push failed \"%s\"\n",
               err->errstr);
        write_response(clnt, RESPONSE_ERROR, err->errstr, 0);
        rc = -1;
        goto closing;
    }

    /* send last row */
    if (first_row) {
        irc = write_response(clnt, RESPONSE_ROW_LAST_DUMMY, NULL, 0);
    } else {
        irc = write_response(clnt, RESPONSE_ROW_LAST, NULL, 0);
    }
    if (irc) {
        errstat_set_rcstrf(err, rc = -1, "Failed to send last row rc %d", irc);
        goto closing;
    }

    if (!irc && rc == CDB2_OK_DONE && clnt->get_cost) {
        rc = _get_remote_cost(clnt, hndl);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to save remote cost rc %d\n", rc);
            goto closing;
        }
    } else {
        rc = 0;
    }

closing:
    cdb2_close(hndl);
reset:
    clnt_plugin_reset(clnt);

    return rc;
}

static int _get_remote_cost(struct sqlclntstate *clnt, cdb2_hndl_tp *hndl)
{
    int rc;

    if (clnt->prev_cost_string) {
        free(clnt->prev_cost_string);
        clnt->prev_cost_string = NULL;
    }

    /* NOTE: the data is coming as sqlite rows */
    rc = cdb2_run_statement(hndl, "SeLeCT comdb2_prevquerycost() as Cost");

    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "Failed to retrieve cost for \"%s\"\n", clnt->sql);
        return -1;
    }

    int ncols = cdb2_numcolumns(hndl);
    if (ncols != 1) {
        logmsg(LOGMSG_ERROR,
               "Incorrect number of columns for cost for \"%s\"\n", clnt->sql);
        return -1;
    }

    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK) {
        logmsg(LOGMSG_ERROR, "Failed to retrieve cost row for \"%s\"\n",
               clnt->sql);
        return -1;
    }

    clnt->fdb_push->row = cdb2_column_value(hndl, 0);
    clnt->fdb_push->rowlen = cdb2_column_size(hndl, 0);
    const char *remcost = (const char *)fdb_push_column_text(clnt, NULL, 0);
    if (remcost) {
        clnt->prev_cost_string = strdup(remcost);
    }
    rc = cdb2_next_record(hndl);
    if (rc != CDB2_OK_DONE) {
        logmsg(LOGMSG_ERROR, "Did get more then one row for cost for \"%s\"\n",
               clnt->sql);
        return -1;
    }

    return 0;
}

static int set_bound_parameters(fdb_push_connector_t *push, cdb2_hndl_tp *hndl,
                                const char *tzname, struct errstat *err,
                                cdb2_client_datetime_t *dts)
{
    struct param_data *p;
    int i;
    int rc;

    for (i= 0; i< push->nparams; i++) {
        p = &push->params[i];
        if (p->pos > 0) {
            /* bind by index */
            switch (p->type) {
            case CLIENT_INT: {
                rc = cdb2_bind_index(hndl, p->pos, CDB2_INTEGER,
                                     p->null ? NULL : &p->u.i, sizeof(p->u.i));
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                    return -1;
                }
            break;
            }
            case CLIENT_REAL: {
                rc = cdb2_bind_index(hndl, p->pos, CDB2_REAL,
                                     p->null ? NULL : &p->u.r, sizeof(p->u.r));
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                    return -1;
                }
            break;
            }
            case CLIENT_BLOB: {
                rc = cdb2_bind_index(hndl, p->pos, CDB2_BLOB,
                                     p->null ? NULL : p->u.p, p->len);
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                    return -1;
                }
            break;
            }
            case CLIENT_CSTR: {
                rc = cdb2_bind_index(hndl, p->pos, CDB2_CSTRING,
                                     p->null ? NULL : p->u.p, p->len);
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                    return -1;
                }
            break;
            }
            case CLIENT_DATETIMEUS: {
                if (p->null) {
                    rc = cdb2_bind_index(hndl, p->pos, CDB2_DATETIMEUS,
                                         NULL, 0);
                } else {
                    cdb2_client_datetimeus_t *dt = (cdb2_client_datetimeus_t *)(dts + i);
                    rc = dttz_to_client_datetimeus(&p->u.dt, tzname, dt);
                    if (rc) {
                        errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                        return -1;
                    }
                    rc = cdb2_bind_index(hndl, p->pos, CDB2_DATETIMEUS,
                                         dt, sizeof(*dt));
                }
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                    return -1;
                }
            break;
            }
            case CLIENT_DATETIME: {
                if (p->null) {
                    rc = cdb2_bind_index(hndl, p->pos, CDB2_DATETIME,
                                         NULL, 0);
                } else {
                    cdb2_client_datetime_t *dt = dts + i;
                    rc = dttz_to_client_datetime(&p->u.dt, tzname, dt);
                    if (rc) {
                        errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                        return -1;
                    }
                    rc = cdb2_bind_index(hndl, p->pos, CDB2_DATETIME,
                                         dt, sizeof(*dt));
                }
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %d", p->pos);
                    return -1;
                }
            break;
            }
            }
        } else {
            /* bind by name */
            switch (p->type) {
            case CLIENT_INT: {
                rc = cdb2_bind_param(hndl, p->name, CDB2_INTEGER,
                                     p->null ? NULL : &p->u.i, sizeof(p->u.i));
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                    return -1;
                }
            break;
            }
            case CLIENT_REAL: {
                rc = cdb2_bind_param(hndl, p->name, CDB2_REAL,
                                     p->null ? NULL : &p->u.r, sizeof(p->u.r));
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                    return -1;
                }
            break;
            }
            case CLIENT_BLOB: {
                rc = cdb2_bind_param(hndl, p->name, CDB2_BLOB,
                                     p->null ? NULL : p->u.p, p->len);
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                    return -1;
                }
            break;
            }
            case CLIENT_CSTR: {
                rc = cdb2_bind_param(hndl, p->name, CDB2_CSTRING,
                                     p->null ? NULL : p->u.p, p->len);
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                    return -1;
                }
            break;
            }
            case CLIENT_DATETIMEUS: {
                if (p->null) {
                    rc = cdb2_bind_param(hndl, p->name, CDB2_DATETIMEUS,
                                         NULL, 0);
                } else {
                    cdb2_client_datetimeus_t *dt = (cdb2_client_datetimeus_t *)(dts + i);
                    rc = dttz_to_client_datetimeus(&p->u.dt, tzname, dt);
                    if (rc) {
                        errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                        return -1;
                    }
                    rc = cdb2_bind_param(hndl, p->name, CDB2_DATETIMEUS,
                                         dt, sizeof(*dt));
                }
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                    return -1;
                }
            break;
            }
            case CLIENT_DATETIME: {
                if (p->null) {
                    rc = cdb2_bind_param(hndl, p->name, CDB2_DATETIME,
                                         NULL, 0);
                } else {
                    cdb2_client_datetime_t *dt = dts + i;
                    rc = dttz_to_client_datetime(&p->u.dt, tzname, dt);
                    if (rc) {
                        errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                        return -1;
                    }
                    rc = cdb2_bind_param(hndl, p->name, CDB2_DATETIME,
                                         dt, sizeof(*dt));
                }
                if (rc) {
                    errstat_set_rcstrf(err, -1, "error binding param %s", p->name);
                    return -1;
                }
            break;
            }
            }
        }
    }

    return 0;
}
