#ifndef XDB_COMMON__H 
#define XDB_COMMON__H 

#define ALCHEMY_VERSION "0.2.1"

#define REDIS_BTREE       5
#define REDIS_LUA_TRIGGER 6

#define OUTPUT_NONE       0 /* this is an error state */
#define OUTPUT_NORMAL     1
#define OUTPUT_PURE_REDIS 2
#define OUTPUT_EMBEDDED   3

#include <endian.h>
#ifndef BYTE_ORDER
  #define BYTE_ORDER LITTLE_ENDIAN
#endif

#define cli redisClient
#define rcommand struct redisCommand


#define bool     unsigned char
#define uchar    unsigned char
#define ushort16 unsigned short
#define uint32   unsigned int
#define ulong    unsigned long
#define lolo     long long
#define ull      unsigned long long
#define uint128  __uint128_t

#define DEFINE_ALCHEMY_HTTP_INFO     \
  typedef struct alchemy_http_info { \
      robj   *file;                  \
      bool    mode;                  \
      bool    ka;                    \
      bool    get;                   \
      bool    post;                  \
      bool    head;                  \
      bool    proto_1_1;             \
      uint32  req_clen;              \
      uint32  retcode;               \
      sds     post_body;             \
      sds     redir;                 \
      list   *req_hdr;               \
      list   *resp_hdr;              \
  } alchemy_http_info;

typedef struct aobj { /* SIZE: 48 BYTES */ //TODO only pass aobj * to functions
    char    *s;
    uint32   len;
    uint32   i;
    ulong    l;
    uint128  x;
    float    f;
    uchar    type;
    uchar    enc;
    uchar    freeme;
    uchar    empty;
} aobj;

typedef struct embedded_row_t {
    int           ncols;
    struct aobj **cols;
} erow_t;

typedef bool select_callback(erow_t* erow);

//TODO move this into a single struct, that can be bzero'ed
#define ALCHEMY_CLIENT_EXTENSIONS           \
    struct sockaddr_in  sa;                 \
    bool                Explain;            \
    sds                 Prepare;            \
    bool                LruColInSelect;     \
    bool                LfuColInSelect;     \
    bool                InternalRequest;    \
    alchemy_http_info   http;               \
    int                 LastJTAmatch;       \
    int                 NumJTAlias;         \
    sds                 bindaddr;           \
    int                 bindport;           \
    select_callback    *scb;

#define ALCHEMY_SERVER_EXTENSIONS \
    long long     stat_num_dirty_commands; /* number of dirty commands    */ \
    unsigned int  delete_miss_pk;          /* PK DELETE hit a MISSing ROW */ \
    unsigned int  delete_miss_fk;          /* FK DELETE hit a MISSing ROW */

#define SHARED_OBJ_DECLARATION \
    robj \
    *singlerow,           *inserted,               *upd8ed, \
    *undefinedcolumntype, *insert_ovrwrt, \
    *toofewcolumns,       *nonuniquecolumns, \
    *nonuniquetablenames, *nonuniqueindexnames,    *indextargetinvalid, \
    *nonuniquekeyname,    *indexedalready,         *index_wrong_nargs, \
    *trigger_wrong_nargs, *luatrigger_wrong_nargs, \
    *nonexistenttable,    *insertcolumn, \
    *nonexistentcolumn,   *nonexistentindex, \
    *invalidrange,        *toofewindicesinjoin,    *toomanyindicesinjoin, \
    *invalidupdatestring, *badindexedcolumnsyntax, \
    *u2big, *col_uint_string_too_long, *col_float_string_too_long, *uint_pkbig,\
    *toomany_nob, \
    *accesstypeunknown, \
    *createsyntax,       *dropsyntax,    *altersyntax,  *alter_other, \
    *lru_other,          *lru_repeat,    *col_lru, \
    *update_lru,         *insert_lru,    *insert_replace_update, \
    *drop_virtual_index, *drop_lru,      *drop_ind_on_sk, \
    *drop_luatrigger, \
    *mci_on_pk,          *UI_SC,         *two_uniq_mci, *uniq_mci_notint, \
    *uniq_mci_pk_notint, \
    *insertsyntax,           *insertsyntax_no_into, *part_insert_other, \
    *insertsyntax_no_values, *luat_decl_fmt,        *luat_c_decl, \
    *key_query_mustbe_eq, \
    *whereclause_in_err,         *where_in_select, \
    *wc_orderby_no_by, \
    *order_by_col_not_found, \
    *oby_lim_needs_num,          *oby_ofst_needs_num, \
    *orderby_count, \
    *selectsyntax,           *selectsyntax_nofrom,   *selectsyntax_nowhere, \
    *deletesyntax,           *deletesyntax_nowhere, \
    *updatesyntax,           *update_pk_range_query, *update_pk_overwrite, \
    *updatesyntax_nowhere, \
    *update_expr,            *update_expr_col, \
    *update_expr_div_0,      *update_expr_mod,       *update_expr_cat, \
    *update_expr_str,        *update_expr_math_str,  *update_expr_col_other, \
    *update_expr_float_overflow,                     *update_expr_empty_str, \
    *up_on_mt_col, \
    *neg_on_uint, \
    *scansyntax,     *scansyntax_noequals, *cr8tbl_scan, \
    *rangequery_index_not_found,                         \
    *whereclause_col_not_indxd, *wc_col_not_found,       \
    *whereclause_no_and,        *whereclause_between,    \
    *joincolumnlisterror,        *fulltablejoin, \
    *join_order_by_syntax,       *join_order_by_tbl,       *join_order_by_col, \
    *join_table_not_in_query,    *joinsyntax_no_tablename, *join_chain, \
    *joindanglingfilter,         *join_noteq,              *join_coltypediff, \
    *join_col_not_indexed,       *join_qo_err,             \
    *createtable_as_on_wrong_type,                         \
    *create_table_err,                                     \
    *create_table_as_count,                                \
    *dump_syntax, *show_syntax,                            \
    *alter_sk_rpt,    *alter_sk_no_i,   *alter_sk_no_lru,  \
    *alter_fk_not_sk, *alter_fk_repeat, *alter_sk_no_lfu,  \
    *select_on_sk,            *scan_sharded,               \
    *constraint_wrong_nargs,  *constraint_col_indexed,     \
    *constraint_not_num,      *constraint_table_mismatch,  \
    *constraint_nonuniq,      *constraint_viol,            \
    *indexobcerr,             *indexobcrpt,                \
    *indexobcill,             *indexcursorerr,             \
    *obindexviol,             *repeat_hash_cnames,         \
    *lfu_other,               *lfu_repeat,                 \
    *drop_lfu,                *col_lfu,                    \
    *insert_lfu,              *kw_cname,                   \
    *dirty_miss,              *evict_other,                \
    *replace_on_dirty_w_inds, *insert_dirty_pkdecl,        \
    *update_on_dirty_w_inds,                               \
    *u128_parse,             *update_u128_complex,         \
    *uniq_simp_index_nums,   *updateipos,                  \
    *join_type_err,          *supported_prepare,           \
    *prepare_syntax,         *execute_argc,                \
    *execute_miss,           *evictnotdirty;

#define DEBUG_C_ARGV(c) \
  for (int i = 0; i < c->argc; i++) \
    printf("%d: argv[%s]\n", i, (char *)c->argv[i]->ptr);

#endif /* XDB_COMMON__H */
