/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2011-2012 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#include "config.h"
#include <sys/types.h>
#include <libcouchbase/couchbase.h>
#include <errno.h>
#include <iostream>
#include <map>
#include <sstream>
#include <queue>
#include <set>
#include <list>
#include <cstring>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <signal.h>
#ifndef WIN32
#include <pthread.h>
#else
#define usleep(n) Sleep(n/1000)
#endif
#include <cstdarg>
#include "common/options.h"
#include "common/histogram.h"
#include <algorithm>

using namespace std;
using namespace cbc;
using namespace cliopts;
using std::vector;
using std::string;

struct DeprecatedOptions {
    UIntOption iterations;
    UIntOption instances;
    BoolOption loop;

    DeprecatedOptions() :
        iterations("iterations"), instances("num-instances"), loop("loop")
    {
        iterations.abbrev('i').hide().setDefault(1000);
        instances.abbrev('Q').hide().setDefault(1);
        loop.abbrev('l').hide().setDefault(false);
    }

    void addOptions(Parser &p) {
        p.addOption(instances);
        p.addOption(loop);
        p.addOption(iterations);
    }
};

class Configuration
{
public:
    Configuration() :
        o_multiSize("batch-size"),
        o_numItems("num-items"),
        o_keyPrefix("key-prefix"),
        o_numThreads("num-threads"),
        o_randSeed("random-seed"),
        o_setPercent("set-pct"),
        o_minSize("min-size"),
        o_maxSize("max-size"),
        o_noPopulate("no-population"),
        o_durability("durability"),
        o_pauseAtEnd("pause-at-end"),
        o_numCycles("num-cycles"),
        o_sequential("sequential"),
        o_startAt("start-at"),
        o_rateLimit("rate-limit"),
        o_tokens("tokens")
    {
        o_multiSize.setDefault(100).abbrev('B').description("Number of operations to batch");
        o_numItems.setDefault(1000).abbrev('I').description("Number of items to operate on");
        o_keyPrefix.abbrev('p').description("key prefix to use");
        o_numThreads.setDefault(1).abbrev('t').description("The number of threads to use");
        o_randSeed.setDefault(0).abbrev('s').description("Specify random seed").hide();
        o_setPercent.setDefault(33).abbrev('r').description("The percentage of operations which should be mutations");
        o_minSize.setDefault(50).abbrev('m').description("Set minimum payload size");
        o_maxSize.setDefault(5120).abbrev('M').description("Set maximum payload size");
        o_noPopulate.setDefault(false).abbrev('n').description("Skip population");
        o_durability.setDefault(false).abbrev('d').description("Perform durability checks");
        o_pauseAtEnd.setDefault(false).abbrev('E').description("Pause at end of run (holding connections open) until user input");
        o_numCycles.setDefault(-1).abbrev('c').description("Number of cycles to be run until exiting. Set to -1 to loop infinitely");
        o_sequential.setDefault(false).description("Use sequential access (instead of random)");
        o_startAt.setDefault(0).description("For sequential access, set the first item");
        o_rateLimit.setDefault(0).abbrev('l').description("Set operations per second limit (per thread)");
        o_tokens.setDefault(0).description("Maximum number of operations in-flight (per thread)");
    }

    void processOptions() {
        opsPerCycle = o_multiSize.result();
        prefix = o_keyPrefix.result();
        setprc = o_setPercent.result();
        shouldPopulate = !o_noPopulate.result();
        durability = o_durability.result();
        setPayloadSizes(o_minSize.result(), o_maxSize.result());

        if (depr.loop.passed()) {
            fprintf(stderr, "The --loop/-l option is deprecated. Use --num-cycles\n");
            maxCycles = -1;
        } else {
            maxCycles = o_numCycles.result();
        }

        if (depr.iterations.passed()) {
            fprintf(stderr, "The --num-iterations/-I option is deprecated. Use --batch-size\n");
            opsPerCycle = depr.iterations.result();
        }
    }

    void addOptions(Parser& parser) {
        parser.addOption(o_multiSize);
        parser.addOption(o_numItems);
        parser.addOption(o_keyPrefix);
        parser.addOption(o_numThreads);
        parser.addOption(o_randSeed);
        parser.addOption(o_setPercent);
        parser.addOption(o_noPopulate);
        parser.addOption(o_durability);
        parser.addOption(o_minSize);
        parser.addOption(o_maxSize);
        parser.addOption(o_pauseAtEnd);
        parser.addOption(o_numCycles);
        parser.addOption(o_sequential);
        parser.addOption(o_startAt);
        parser.addOption(o_rateLimit);
        parser.addOption(o_tokens);
        params.addToParser(parser);
        depr.addOptions(parser);
    }

    ~Configuration() {
        delete []static_cast<char *>(data);
    }

    void setPayloadSizes(uint32_t minsz, uint32_t maxsz) {
        if (minsz > maxsz) {
            minsz = maxsz;
        }

        minSize = minsz;
        maxSize = maxsz;

        if (data) {
            delete []static_cast<char *>(data);
        }

        data = static_cast<void *>(new char[maxSize]);
        /* fill data array with pattern */
        uint32_t *iptr = static_cast<uint32_t *>(data);
        for (uint32_t ii = 0; ii < maxSize / sizeof(uint32_t); ++ii) {
            iptr[ii] = 0xdeadbeef;
        }
        /* pad rest bytes with zeros */
        size_t rest = maxSize % sizeof(uint32_t);
        if (rest > 0) {
            char *cptr = static_cast<char *>(data) + (maxSize / sizeof(uint32_t));
            memset(cptr, 0, rest);
        }
    }

    uint32_t getNumInstances(void) {
        if (depr.instances.passed()) {
            return depr.instances.result();
        }
        return o_numThreads.result();
    }

    bool isTimings(void) { return params.useTimings(); }

    bool isLoopDone(size_t niter) {
        if (maxCycles == -1) {
            return false;
        }
        return niter >= (size_t)maxCycles;
    }

    void setDGM(bool val) {
        dgm = val;
    }

    void setWaitTime(uint32_t val) {
        waitTime = val;
    }

    uint32_t getRandomSeed() { return o_randSeed; }
    uint32_t getNumThreads() { return o_numThreads; }
    string& getKeyPrefix() { return prefix; }
    bool shouldPauseAtEnd() { return o_pauseAtEnd; }
    bool sequentialAccess() { return o_sequential; }
    unsigned firstKeyOffset() { return o_startAt; }
    uint32_t getNumItems() { return o_numItems; }
    uint32_t getRateLimit() { return o_rateLimit; }
    uint32_t getTokens() { return o_tokens; }

    void *data;

    uint32_t opsPerCycle;
    unsigned setprc;
    string prefix;
    uint32_t maxSize;
    uint32_t minSize;
    volatile int maxCycles;
    bool dgm;
    bool shouldPopulate;
    bool durability;
    uint32_t waitTime;
    ConnParams params;

private:
    UIntOption o_multiSize;
    UIntOption o_numItems;
    StringOption o_keyPrefix;
    UIntOption o_numThreads;
    UIntOption o_randSeed;
    UIntOption o_setPercent;
    UIntOption o_minSize;
    UIntOption o_maxSize;
    BoolOption o_noPopulate;
    BoolOption o_durability;
    BoolOption o_pauseAtEnd; // Should pillowfight pause execution (with
                             // connections open) before exiting?
    IntOption o_numCycles;
    BoolOption o_sequential;
    UIntOption o_startAt;
    UIntOption o_rateLimit;
    UIntOption o_tokens;
    DeprecatedOptions depr;
} config;

void log(const char *format, ...)
{
    char buffer[512];
    va_list args;

    va_start(args, format);
    vsprintf(buffer, format, args);
    if (config.isTimings()) {
        std::cerr << "[" << std::fixed << lcb_nstime() / 1000000000.0 << "] ";
    }
    std::cerr << buffer << std::endl;
    va_end(args);
}



extern "C" {
static void operationCallback(lcb_t, int, const lcb_RESPBASE*);
static void storeCallback(lcb_t, int, const lcb_RESPBASE*);
static void durability_callback(lcb_t instance, const void *cookie,
                                lcb_error_t error,const lcb_durability_resp_t *resp);
}

class InstanceCookie {
public:
    InstanceCookie(lcb_t instance) {
        lcb_set_cookie(instance, this);
        lastPrint = 0;
        if (config.isTimings()) {
            hg.install(instance, stdout);
        }
    }

    static InstanceCookie* get(lcb_t instance) {
        return (InstanceCookie *)lcb_get_cookie(instance);
    }


    static void dumpTimings(lcb_t instance, const char *header, bool force=false) {
        time_t now = time(NULL);
        InstanceCookie *ic = get(instance);

        if (now - ic->lastPrint > 0) {
            ic->lastPrint = now;
        } else if (!force) {
            return;
        }

        Histogram &h = ic->hg;
        printf("[%f %s]\n", lcb_nstime() / 1000000000.0, header);
        printf("              +---------+---------+---------+---------+\n");
        h.write();
        printf("              +----------------------------------------\n");
    }

private:
    time_t lastPrint;
    Histogram hg;
};

struct NextOp {
    NextOp() : seqno(0), valsize(0), isStore(false) {}

    string key;
    uint32_t seqno;
    size_t valsize;
    bool isStore;
};

class KeyGenerator {
public:
    KeyGenerator(int ix) :
        currSeqno(0), rnum(0), ngenerated(0), isSequential(false),
        isPopulate(config.shouldPopulate)
{
        srand(config.getRandomSeed());
        for (int ii = 0; ii < 8192; ++ii) {
            seqPool[ii] = rand();
        }
        if (isPopulate) {
            isSequential = true;
        } else {
            isSequential = config.sequentialAccess();
        }


        // Maximum number of keys for this thread
        maxKey = config.getNumItems() /  config.getNumThreads();

        offset = config.firstKeyOffset();
        offset += maxKey * ix;
        id = ix;
    }

    void setNextOp(NextOp& op) {
        bool store_override = false;

        if (isPopulate) {
            if (++ngenerated < maxKey) {
                store_override = true;
            } else {
                printf("Thread %d has finished populating.\n", id);
                isPopulate = false;
                isSequential = config.sequentialAccess();
            }
        }

        if (isSequential) {
            rnum++;
            rnum %= maxKey;
        } else {
            rnum += seqPool[currSeqno];
            currSeqno++;
            if (currSeqno > 8191) {
                currSeqno = 0;
            }
        }

        op.seqno = rnum;

        if (store_override) {
            op.isStore = true;
        } else {
            op.isStore = shouldStore(op.seqno);
        }

        if (op.isStore) {
            size_t size;
            if (config.minSize == config.maxSize) {
                size = config.minSize;
            } else {
                size = config.minSize + op.seqno % (config.maxSize - config.minSize);
            }
            op.valsize = size;
        }
        generateKey(op);
    }

    bool shouldStore(uint32_t seqno) {
        if (config.setprc == 0) {
            return false;
        }

        float seqno_f = seqno % 100;
        float pct_f = seqno_f / config.setprc;
        return pct_f < 1;
    }

    void generateKey(NextOp& op) {
        uint32_t seqno = op.seqno;
        seqno %= maxKey;
        seqno += offset-1;

        char buffer[21];
        snprintf(buffer, sizeof(buffer), "%020d", seqno);
        op.key.assign(config.getKeyPrefix() + buffer);
    }
    const char *getStageString() const {
        if (isPopulate) {
            return "Populate";
        } else {
            return "Run";
        }
    }

private:
    uint32_t seqPool[8192];
    uint32_t currSeqno;
    uint32_t rnum;
    uint32_t offset;
    uint32_t maxKey;
    size_t ngenerated;
    int id;

    bool isSequential;
    bool isPopulate;
};

class ThreadContext
{
public:
    ThreadContext(lcb_t handle, int ix) : kgen(ix), niter(0), instance(handle) {
        dur_options.version = 0;
        dur_options.v.v0.persist_to = 0;
        dur_options.v.v0.replicate_to = 1;
        dur_options.v.v0.timeout = 1000 * 1000 * 5;
        dur_options.v.v0.interval = 1000 * 100;//0 * 1;
    }

    void singleLoop()
    {
        bool hasItems = false;
        lcb_sched_enter(instance);
        NextOp opinfo;
        dur_options.version = 0;
        dur_options.v.v0.persist_to = 0;
        dur_options.v.v0.replicate_to = 1;
        dur_options.v.v0.timeout = 1000 * 1000 * 5;
        dur_options.v.v0.interval = 1000 * 100;//0 * 1;

        for (size_t ii = 0; ii < config.opsPerCycle; ++ii) {
            hasItems = scheduleNextOperation();
        }
        if (hasItems)
        {
            lcb_sched_leave(instance);
            lcb_U64 store_dispatch_time = lcb_nstime();
            lcb_wait(instance);

            if (error != LCB_SUCCESS)
            {
                log("Operation(s) failed: [0x%x] %s", error, lcb_strerror(instance, error));
            }

            // display durability metrics once per batch size.
            if (config.durability) {
                printDurabilityStatistics();
            }
        } else {
            lcb_sched_fail(instance);
        }
    }

    void printDurabilityStatistics() {
        // Sort our complete timings, convert ns to ms, then dump them to screen.
        std::sort(completed_timings.begin(), completed_timings.end());
        
        double median = completed_timings[completed_timings.size() / 2] / 1e6;
        double percentile_95 = completed_timings[(95 * completed_timings.size()) / 100] / 1e6;
        double percentile_99 = completed_timings[(99 * completed_timings.size()) / 100] / 1e6;
        
        printf("%lld Store+replicate - Median: %0.2f ms, 95%%: %0.2f ms, 99%%: %0.2f ms\n",
               lcb_nstime(), median, percentile_95, percentile_99);

        completed_timings.clear();
    }

    bool scheduleNextOperation() {
        NextOp opinfo;

        kgen.setNextOp(opinfo);
        if (opinfo.isStore)
        {
            lcb_CMDSTORE scmd = { 0 };
            scmd.operation = LCB_SET;
            LCB_CMD_SET_KEY(&scmd, opinfo.key.c_str(), opinfo.key.size());
            LCB_CMD_SET_VALUE(&scmd, config.data, opinfo.valsize);
            error = lcb_store3(instance, this, &scmd);
            in_flight_timings.insert(std::make_pair(opinfo.key, lcb_nstime()));
        }
        else
        {
            lcb_CMDGET gcmd = { 0 };
            LCB_CMD_SET_KEY(&gcmd, opinfo.key.c_str(), opinfo.key.size());
            error = lcb_get3(instance, this, &gcmd);
        }
        if (error != LCB_SUCCESS) {
            log("Failed to schedule operation: [0x%x] %s", error, lcb_strerror(instance, error));
            return false;
        } else {
            return true;
        }
    }

    void spoolOperations() {
        lcb_sched_enter(instance);
        while (tokens > 0) {
            if (scheduleNextOperation()) {
                tokens--;
            }
        }
        lcb_sched_leave(instance);
    }

    void runAsync() {
        tokens = config.getTokens();

        // Spool up as many ops as we have tokens
        spoolOperations();

        // Wait until requested operations are complete.
        do {
            lcb_wait(instance);
        } while (!config.isLoopDone(niter));
    }

    bool run() {
        lcb_U64 previous_time = lcb_nstime();
        do {
            singleLoop();

            if (config.isTimings()) {
                InstanceCookie::dumpTimings(instance, kgen.getStageString());
            }
            if (config.params.shouldDump()) {
                lcb_dump(instance, stderr, LCB_DUMP_ALL);
            }
            if (config.getRateLimit() > 0) {
                lcb_U64 now = lcb_nstime();
                const lcb_U64 elapsed_ns = now - previous_time;
                const lcb_U64 wanted_duration_ns = config.opsPerCycle * 1e9 / config.getRateLimit();
                if (elapsed_ns < wanted_duration_ns) {
                    // Dampen the sleep time by averaging with the previous
                    // sleep time.
                    static lcb_U64 last_sleep_ns = 0;
                    const lcb_U64 sleep_ns =
                            (last_sleep_ns + wanted_duration_ns - elapsed_ns) / 2;
                    usleep(sleep_ns / 1000);
                    now += sleep_ns;
                    last_sleep_ns = sleep_ns;
                }
                previous_time = now;
            }

        } while (!config.isLoopDone(++niter));

        if (config.isTimings()) {
            InstanceCookie::dumpTimings(instance, kgen.getStageString(), true);
        }
        return true;
    }

#ifndef WIN32
    pthread_t thr;
#endif

protected:
    // the callback methods needs to be able to set the error handler..
    friend void operationCallback(lcb_t, int, const lcb_RESPBASE*);
    friend void storeCallback(lcb_t, int, const lcb_RESPBASE*);
    friend void durability_callback(lcb_t, const void *, lcb_error_t, const lcb_durability_resp_t *);

    Histogram histogram;

    void setError(lcb_error_t e) { error = e; }

    void setKeyReplicated(const char* key) {
        std::map<std::string, lcb_U64>::iterator it = in_flight_timings.find(key);
        assert(it != in_flight_timings.end());
        lcb_U64 duration = lcb_nstime() - it->second;
        in_flight_timings.erase(it);

        completed_timings.push_back(duration);
        tokens++;
    }

private:
    KeyGenerator kgen;
    size_t niter;
    lcb_error_t error;
    lcb_t instance;
    int tokens;

    // DH
    lcb_durability_opts_t dur_options = { 0 };
    // Map of key -> store start time.
    std::map<std::string, lcb_U64> in_flight_timings;

    // (unordered) vector of store+replicate timings for completed ops.
    std::vector<lcb_U64> completed_timings;
}; //ThreadContext


static void updateOpsPerSecDisplay()
{
#ifndef WIN32

    static time_t start_time = time(NULL);
    static int is_tty = isatty(STDOUT_FILENO);
    if (is_tty) {
        static volatile unsigned long nops = 0;
        if (++nops % 10000 == 0) {
            time_t now = time(NULL);
            time_t nsecs = now - start_time;
            if (!nsecs) { nsecs = 1; }
            unsigned long ops_sec = nops / nsecs;
            printf("OPS/SEC: %10lu\r", ops_sec);
            fflush(stdout);
        }
    }
#endif
}

static void operationCallback(lcb_t, int, const lcb_RESPBASE *resp)
{
    ThreadContext *tc;
    tc = const_cast<ThreadContext *>(reinterpret_cast<const ThreadContext *>(resp->cookie));
    tc->setError(resp->rc);

    updateOpsPerSecDisplay();
}

static void storeCallback(lcb_t, int, const lcb_RESPBASE *resp)
{
    ThreadContext *tc;
    tc = const_cast<ThreadContext *>(reinterpret_cast<const ThreadContext *>(resp->cookie));
    tc->setError(resp->rc);

    if (resp->rc != LCB_SUCCESS) {
         fprintf( stderr, "Store Failed: %s\n", lcb_strerror(NULL, resp->rc));
    }

    if (config.durability) {
        // Schedule a durability callback for the store.
        lcb_durability_cmd_t endure = { 0 };
        endure.v.v0.key = resp->key;
        endure.v.v0.nkey = resp->nkey;
        endure.v.v0.cas = resp->cas;
        //fprintf(stderr,"Key %-22.22s has CAS: %p\n",resp->key, resp->cas );

        const lcb_durability_cmd_t *cmdlist = &endure;
        lcb_error_t ret = lcb_durability_poll(tc->instance, tc, &(tc->dur_options),
                                              1, &cmdlist);
        if (ret != LCB_SUCCESS) {
            fprintf( stderr, "Poll Failed: %s\n", lcb_strerror(NULL, ret));
            exit(1);
        }
    } else {
        if (config.isLoopDone(++tc->niter)) {
            // Done
            lcb_breakout(tc->instance);
        } else {
            // Schedule more operations (if we have enough tokens)
            if (tc->tokens >= config.opsPerCycle) {
                tc->spoolOperations();
            }
        }
    }
    updateOpsPerSecDisplay();
}

//DH
static void durability_callback(lcb_t instance, const void *cookie,
                                lcb_error_t error,const lcb_durability_resp_t *resp)
{
    (void)instance;
    (void)error;

    // Expect to replicate to one other node.
    if (resp->v.v0.nreplicated != 1)
    {
        fprintf(stderr,"nreplicated: expected 1, got %d\n", resp->v.v0.nreplicated);
    }

    ThreadContext *tc;
    tc = const_cast<ThreadContext *>(reinterpret_cast<const ThreadContext *>(cookie));
    tc->setKeyReplicated((const char*)resp->v.v0.key);

    if (resp->v.v0.err == LCB_SUCCESS) {
//        fprintf(stderr,"Key %s was endured with CAS %p after %d tries!\n", resp->v.v0.key, resp->v.v0.cas, resp->v.v0.nresponses );
    }
    else
    {
        switch (resp->v.v0.err )
        {
            case LCB_KEY_ENOENT:
                fprintf(stderr,"Key %s not found on server?: %s\n",resp->v.v0.key, lcb_strerror(NULL, resp->v.v0.err));
                break;
            default:
                fprintf(stderr,"Key %s did not endure: %s\n",resp->v.v0.key,lcb_strerror(NULL, resp->v.v0.err));
                break;
        }
    }

    if (config.isLoopDone(++tc->niter)) {
        // Done
        lcb_breakout(tc->instance);
    } else {
        // Schedule more operations (if we have enough tokens)
        if (tc->tokens >= config.opsPerCycle) {
            tc->spoolOperations();
        }
    }

    // TODO: make configurable.
    if (tc->niter % 10000 == 0) {
        tc->printDurabilityStatistics();
    }
}

std::list<ThreadContext *> contexts;

extern "C" {
    typedef void (*handler_t)(int);
}

#ifndef WIN32
static void sigint_handler(int)
{
    static int ncalled = 0;
    ncalled++;

    if (ncalled < 2) {
        log("Termination requested. Waiting threads to finish. Ctrl-C to force termination.");
        signal(SIGINT, sigint_handler); // Reinstall
        config.maxCycles = 0;
        return;
    }

    std::list<ThreadContext *>::iterator it;
    for (it = contexts.begin(); it != contexts.end(); ++it) {
        delete *it;
    }
    contexts.clear();
    exit(EXIT_FAILURE);
}

static void setup_sigint_handler()
{
    struct sigaction action;
    sigemptyset(&action.sa_mask);
    action.sa_handler = sigint_handler;
    action.sa_flags = 0;
    sigaction(SIGINT, &action, NULL);
}

extern "C" {
static void* thread_worker(void*);
}

static void start_worker(ThreadContext *ctx)
{
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    int rc = pthread_create(&ctx->thr, &attr, thread_worker, ctx);
    if (rc != 0) {
        log("Couldn't create thread: (%d)", errno);
        exit(EXIT_FAILURE);
    }
}
static void join_worker(ThreadContext *ctx)
{
    void *arg = NULL;
    int rc = pthread_join(ctx->thr, &arg);
    if (rc != 0) {
        log("Couldn't join thread (%d)", errno);
        exit(EXIT_FAILURE);
    }
}

#else
static void setup_sigint_handler() {}
static void start_worker(ThreadContext *ctx) { ctx->run(); }
static void join_worker(ThreadContext *ctx) { (void)ctx; }
#endif

extern "C" {
static void *thread_worker(void *arg)
{
    ThreadContext *ctx = static_cast<ThreadContext *>(arg);
    if (config.getTokens() > 0) {
        ctx->runAsync();
    } else {
        ctx->run();
    }
    return NULL;
}
}

int main(int argc, char **argv)
{
    int exit_code = EXIT_SUCCESS;
    setup_sigint_handler();

    Parser parser("cbc-pillowfight");
    config.addOptions(parser);
    parser.parse(argc, argv, false);
    config.processOptions();
    size_t nthreads = config.getNumThreads();
    log("Running. Press Ctrl-C to terminate...");

#ifdef WIN32
    if (nthreads > 1) {
        log("WARNING: More than a single thread on Windows not supported. Forcing 1");
        nthreads = 1;
    }
#endif

    struct lcb_create_st options;
    ConnParams& cp = config.params;
    lcb_error_t error;

    for (uint32_t ii = 0; ii < nthreads; ++ii) {
        cp.fillCropts(options);
        lcb_t instance = NULL;
        error = lcb_create(&instance, &options);
        if (error != LCB_SUCCESS) {
            log("Failed to create instance: %s", lcb_strerror(NULL, error));
            exit(EXIT_FAILURE);
        }
        lcb_install_callback3(instance, LCB_CALLBACK_STORE, storeCallback);
        lcb_install_callback3(instance, LCB_CALLBACK_GET, operationCallback);
        // DH
        lcb_set_durability_callback(instance, durability_callback);

        cp.doCtls(instance);

        new InstanceCookie(instance);


        lcb_connect(instance);
        lcb_wait(instance);

        error = lcb_get_bootstrap_status(instance);

        if (error != LCB_SUCCESS) {
            std::cout << std::endl;
            log("Failed to connect: %s", lcb_strerror(instance, error));
            exit(EXIT_FAILURE);
        }

        ThreadContext *ctx = new ThreadContext(instance, ii);
        contexts.push_back(ctx);
        start_worker(ctx);
    }

    for (std::list<ThreadContext *>::iterator it = contexts.begin();
            it != contexts.end(); ++it) {
        join_worker(*it);
    }
    return exit_code;
}
