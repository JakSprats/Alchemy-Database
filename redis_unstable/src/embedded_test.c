
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <strings.h>

#include "redis.h"

#include "embed.h"

//GLOBALS
extern eresp_t *CurrEresp; // USED in callbacks to get "CurrEresp->cnames[]"

// PROTOTYPES
typedef int printer(const char *format, ...);
struct aobj;
void dumpAobj(printer *prn, struct aobj *a);

// CALLBACKS
sds Col1_seq = NULL;
void initValidation() {
    if (Col1_seq) sdsfree(Col1_seq); Col1_seq = NULL;
}
void addCol1toValidation(unsigned int i) {
    if (Col1_seq) Col1_seq = sdscatprintf(Col1_seq,     "_%d", i);
    else          Col1_seq = sdscatprintf(sdsempty(), "%d",  i);
}

static bool print_callback(erow_t* er) {
    printf("\tROW:\n");
    for (int i = 0; i < er->ncols; i++) {
        if (!i) addCol1toValidation(er->cols[i]->i);
        printf("\t\t"); dumpAobj(printf, er->cols[i]);
    }
    return 1;
}
static void print_selected_rows(eresp_t *ersp) {
    printf("\tQUERY:\n");
    for (int i = 0; i < ersp->ncols; i++) {
        printf("\t\tCOLUMN[%d].NAME: %s\n", i, ersp->cnames[i]);
    }
}
static bool print_callback_w_cnames(erow_t* er) {
    printf("\tROW:\n");
    for (int i = 0; i < er->ncols; i++) {
        if (!i) addCol1toValidation(er->cols[i]->i);
        printf("\t\t%s: ", CurrEresp->cnames[i]); dumpAobj(printf, er->cols[i]);
    }
    return 1;
}

static int NumRowsToPrint = 9999;
static int NumRowsPrinted = 0;
static void setNumRowsToPrint(int n) { NumRowsToPrint = n; NumRowsPrinted = 0; }
static bool num_print_callback(erow_t* er) { //printf("num_print_callback\n");
    if (NumRowsToPrint == NumRowsPrinted) return 0;
    NumRowsPrinted++;
    printf("\tROW:\n");
    for (int i = 0; i < er->ncols; i++) {
        if (!i) addCol1toValidation(er->cols[i]->i);
        printf("\t\t"); dumpAobj(printf, er->cols[i]);
    }
    return 1;
}

// TESTS
static eresp_t *run_test(char *msg, char *sql,
                         select_callback *scb, int ivld, char *pkvld,
                         bool debug) {
    initValidation();
    if (msg) printf("%s: ", msg);
    printf("TEST: %s\n", sql);
    eresp_t *ersp = e_alchemy_raw(sql, scb);
    if (debug) printEmbedResp(ersp);
    if (ivld != -1 && ersp->objs[0]->l != (unsigned long)ivld) {
        printf("INTEGER CHECK FAILED, GOT: %lu, EXPECTED: %d\n",
                ersp->objs[0]->l, ivld);
        exit(-1);
    }
    if (pkvld && strcmp(Col1_seq, pkvld)) {
        printf("STRING CHECK FAILED, GOT: (%s), EXPECTED: (%s)\n",
                Col1_seq, pkvld);
        exit(-1);
    }
    return ersp;
}

static void testRedisEmbedded();

static void populate_foo(bool ddl_dbg, bool crud_dbg) {
    run_test(NULL,"CREATE TABLE foo (pk INT, fk INT, val TEXT)",
             NULL, -1, NULL, ddl_dbg);

    run_test(NULL,"INSERT INTO foo VALUES (1, 100, '1111x11111')",
             NULL, -1, NULL, crud_dbg);

    run_test(NULL,"INSERT INTO foo VALUES (2, 100, '2222y22222')",
             NULL, -1, NULL, crud_dbg);

    run_test(NULL,"INSERT INTO foo VALUES (3, 200, '3333z33333')",
             NULL, -1, NULL, crud_dbg);

    // INSERT ERROR
    run_test("ERROR", "INSERT INTO foo VALUES (1, '11111x11111')",
             NULL, -1, NULL, crud_dbg);

    run_test(NULL, "CREATE INDEX i_foo ON foo (fk)",
             NULL, -1, NULL, ddl_dbg);

    run_test(NULL, "DESC foo", NULL, -1, NULL, 1);
    run_test(NULL, "DUMP foo", NULL, -1, NULL, 1);
}
static void addrows_to_foo(bool crud_dbg) {
    run_test(NULL, "INSERT INTO foo VALUES (4, 100, '4444a44444')",
             NULL, -1, NULL, crud_dbg);
    run_test(NULL, "INSERT INTO foo VALUES (5, 100, '5555b55555')",
             NULL, -1, NULL, crud_dbg);
    run_test(NULL, "INSERT INTO foo VALUES (6, 200, '6666b66666')",
             NULL, -1, NULL, crud_dbg);
}
static void populate_bar(bool ddl_dbg, bool crud_dbg) {
    run_test(NULL, "CREATE TABLE bar (pk INT, fk INT, val TEXT)",
             NULL, -1, NULL, ddl_dbg);
    run_test(NULL, "INSERT INTO bar VALUES (1, 100, 'bar_ONE')",
             NULL, -1, NULL, crud_dbg);
    run_test(NULL, "INSERT INTO bar VALUES (2, 200, 'bar_TWO')",
             NULL, -1, NULL, crud_dbg);
    run_test(NULL, "CREATE INDEX i_bar ON bar (fk)",
             NULL, -1, NULL, ddl_dbg);
}

static void test_scan(bool dbg) {
    run_test(NULL, "SCAN * FROM foo", print_callback, -1, "1_2_3_4_5_6", dbg);

    run_test(NULL, "SCAN COUNT(*) FROM foo", NULL, 6, NULL, dbg);

    run_test(NULL, "SCAN * FROM foo ORDER BY val DESC",
              print_callback_w_cnames, -1, "6_5_4_3_2_1", dbg);

    run_test(NULL, "SCAN * FROM foo f, bar b WHERE f.fk = b.fk",
              print_callback_w_cnames, -1, "1_2_4_5_3_6", dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS", "SCAN * FROM foo f, bar b WHERE f.fk = b.fk",
              num_print_callback, -1, "1_2_4", dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS",
             "SCAN * FROM foo f, bar b WHERE f.fk = b.fk ORDER BY f.val DESC",
             num_print_callback, -1, "6_5_4", dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS", "SCAN * FROM foo", num_print_callback, -1, "1_2_3", dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS", "SCAN * FROM foo ORDER BY val DESC",
              num_print_callback, -1, "6_5_4", dbg);
}

int main(int argc, char **argv) {
    argc = 0; argv = NULL; /* compiler warning */
    bool dbg      = 0;
    bool ddl_dbg  = 1;
    bool crud_dbg = 1;

    populate_foo(ddl_dbg, crud_dbg);

    eresp_t *ersp = run_test(NULL, "SELECT * FROM foo WHERE fk = 100",
                             print_callback, -1, "1_2", dbg);
    print_selected_rows(ersp);

    run_test(NULL, "SELECT COUNT(*) FROM foo WHERE fk = 100",
             NULL, 2, NULL, dbg);

    run_test(NULL, "SELECT * FROM foo WHERE fk = 100 ORDER BY val DESC",
             print_callback, -1, "2_1", dbg);

    populate_bar(ddl_dbg, crud_dbg);

    run_test(NULL,
            "SELECT * FROM foo f, bar b WHERE f.fk = b.fk AND b.fk = 100",
             print_callback, -1, "1_2", dbg);

    run_test(NULL,
            "SELECT * FROM foo f, bar b WHERE f.fk = b.fk AND b.fk = 200",
             print_callback, -1, "3", dbg);

    run_test(NULL,
      "SELECT COUNT(*) FROM foo f, bar b WHERE f.fk = b.fk AND b.fk = 100",
             NULL, 2, NULL, dbg);

    run_test(NULL,
      "SELECT * FROM foo f, bar b WHERE f.fk = b.fk AND b.fk = 100 ORDER BY f.val DESC",
             print_callback_w_cnames, -1, "2_1", dbg);

    addrows_to_foo(crud_dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS", "SELECT * FROM foo WHERE fk = 100",
             num_print_callback, -1, "1_2_4", dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS", "SELECT * FROM foo WHERE fk = 100 ORDER BY val DESC",
             num_print_callback, -1, "5_4_2", dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS",
             "SELECT * FROM foo f, bar b WHERE f.fk = b.fk AND b.fk = 100",
             num_print_callback, -1, "1_2_4", dbg);

    setNumRowsToPrint(3);
    run_test("3 ROWS",
      "SELECT * FROM foo f, bar b WHERE f.fk = b.fk AND b.fk = 100 ORDER BY f.val DESC",
             num_print_callback, -1, "5_4_2", dbg);

    test_scan(dbg);

    run_test(NULL, "SCAN fk FROM foo",
            print_callback, -1, "100_100_200_100_100_200", dbg);
    run_test(NULL, "UPDATE foo SET fk = 300 WHERE fk = 100",
             NULL, 4, NULL, crud_dbg);
    run_test(NULL, "SCAN fk FROM foo",
            print_callback, -1, "300_300_200_300_300_200", dbg);

    run_test(NULL, "DELETE FROM foo WHERE fk = 200",
             NULL, 2, NULL, crud_dbg);
    run_test(NULL, "SCAN fk FROM foo",
            print_callback, -1, "300_300_300_300", dbg);
    //testRedisEmbedded();

    printf("exiting normally\n");
    return 0;
}


static void testRedisEmbedded() {
    int    rargc = 3;
    robj **rargv = zmalloc(sizeof(robj *) * rargc);
    rargv[0] = createStringObject("LPUSH", strlen("LPUSH"));
    rargv[1] = createStringObject("list1", strlen("list1"));
    rargv[2] = createStringObject("CAT named Felix", strlen("CAT named Felix"));
    printf("TEST LPUSH 1\n");
    eresp_t *ersp = e_alchemy(rargc, rargv, print_callback);

    rargc = 3;
    rargv = zmalloc(sizeof(robj *) * rargc);
    rargv[0] = createStringObject("LPUSH", strlen("LPUSH"));
    rargv[1] = createStringObject("list1", strlen("list1"));
    rargv[2] = createStringObject("DOG named Spot", strlen("DOG named Spot"));
    printf("TEST LPUSH 1\n");
    ersp = e_alchemy(rargc, rargv, print_callback);
    rargc = 3;
    rargv = zmalloc(sizeof(robj *) * rargc);
    rargv[0] = createStringObject("LPUSH", strlen("LPUSH"));
    rargv[1] = createStringObject("list1", strlen("list1"));
    rargv[2] = createStringObject("OWL named Whoo", strlen("OWL named Whoo"));
    printf("TEST LPUSH 1\n");
    ersp = e_alchemy(rargc, rargv, print_callback);

    rargc = 4;
    rargv = zmalloc(sizeof(robj *) * rargc);
    rargv[0] = createStringObject("LRANGE", strlen("LRANGE"));
    rargv[1] = createStringObject("list1", strlen("list1"));
    rargv[2] = createStringObject("0", strlen("0"));
    rargv[3] = createStringObject("-1", strlen("-1"));
    printf("TEST LRANGE (all)\n");
    ersp = e_alchemy(rargc, rargv, print_callback);
}
