#ifndef DEDUPOP_H
#define DEDUPOP_H

#include "boost/algorithm/string.hpp"
#include "include/rados/librados.hpp"
#include "include/types.h"
#include "osd_types.h"

#include "OpRequest.h"
#include "PG.h"

#include <algorithm>
#include <chrono>
#include <list>
#include <ctime>
#include <map>

//hash
void MurmurHash3_x86_32 (const void * key, int len,
    uint32_t seed, void * out);
void MurmurHash3_x86_128 (const void * key, const int len,
    uint32_t seed, void * out);
void MurmurHash3_x64_128 (const void * key, const int len,
    const uint32_t seed, void * out);
    

class DedupRadosOps 
{
private:
    librados::IoCtx io_ctx;
    std::list <librados::AioCompletion*> comps;
    std::list <librados::bufferlist*> bufs;

    std::string murmur_hash(char*, uint32_t);

    int error;
public:
    ~DedupRadosOps();
    DedupRadosOps(){};
    DedupRadosOps(librados::Rados*);

    int get_return_value();
    void create_ctx(librados::Rados*);
    void issue_one_full_read(std::string, librados::bufferlist*);
    void issue_multiple_hashed_writes(librados::bufferlist*, uint32_t);
    void issue_multiple_split_reads(std::string);
    void issue_single_write (std::string, librados::bufferlist*);
    void concatenate_bufs(librados::bufferlist*);
    bool is_complete();
};


class DedupOpBase 
{
public:
    OpRequestRef op;
    spg_t pg;

    enum State { START, READING, WRITING, DONE, READING_RECIPE, 
        WRITING_RECIPE, SLEEPING, SLEEP, 
        WAIT_WRITING_SLEEP, WAIT_WRITING_SLEEPING };
    State state;

    DedupRadosOps* dedup_ops;
    librados::Rados* rados;    
    librados::bufferlist data;
    std::chrono::time_point<std::chrono::system_clock> timer;    
    std::string name;
    uint32_t chunk_sz;
    

public:
    DedupOpBase(spg_t&, OpRequestRef&, uint32_t);
    virtual ~DedupOpBase();
    bool is_ready();
    void set_data(std::string, uint32_t, librados::Rados*);

    virtual void queue_back() = 0;
    virtual bool is_done() = 0;
    virtual void execute() = 0;
    virtual bool is_write() = 0;
};


class DedupOpInlineWrite : public DedupOpBase 
{
public:
    DedupOpInlineWrite(spg_t pg, OpRequestRef& op, uint32_t time)
        : DedupOpBase (pg, op,time)
    {
    }

    void execute();
    bool is_done();
    void queue_back();
    bool is_write() { return true; }
};

class DedupOpPostProcessWrite : public DedupOpBase {
public:
    DedupOpPostProcessWrite(spg_t pg, OpRequestRef& op, uint32_t time)
        : DedupOpBase (pg, op, time)
    {
    }

    void execute();
    bool is_done();
    void queue_back();
    bool is_write() { return true; }
};

class DedupOpRead : public DedupOpBase {
public:
    DedupOpRead(spg_t pg, OpRequestRef& op)
        : DedupOpBase (pg, op, 0)
    {
    }

    void execute();
    bool is_done();
    void queue_back();
    bool is_write() { return false; }
};

#endif
