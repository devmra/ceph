//#include <rados/librados.hpp>
#include "common/Mutex.h"
#include "include/rados/librados.hpp"
#include "osd/DedupOp.h"
#include "osd/OSD.h"
#include "DedupLogger.h"
#include "time_stats.h"
#include <list>
#include <map>
#include <queue>
#include <mutex>

#include "atomicops.h"
#include "readerwriterqueue.h"
#include <chrono>

#define PRINT_STATS 1



class Deduper
{
private:
    librados::Rados rados;

    moodycamel::ReaderWriterQueue<DedupOpBase*> waiting_ops2;

    std::list<DedupOpBase*> issued_ops;
    std::map<std::string, librados::bufferlist*> cached_reads;
    std::set<std::string> skip_writes;

public:
    Mutex waiting_lock;
    Mutex cache_lock;
    Mutex skip_lock;
    Mutex q_lock;

    time_stats_t rd_data, rd_chunks, rd_concat_cache, rd_bl;
    time_stats_t wt_read, wt_chunks, wt_recipe;
    time_stats_t filter_oh, ceph_oh;
    time_stats_t reg_rd, reg_write;

    void skip_write(std::string);
    bool check_skip_write(std::string);

    bool filter_op(PGRef pg, OpRequestRef& op);
    void cache_read(std::string, librados::bufferlist*);
    librados::bufferlist* get_cached_read(std::string);

    void flush_stats();
    void reset_stats();

    //thread loop
    static void* run(void*);

//singleton stuff
private: 
    Deduper();

public:
    Deduper(Deduper const&)        = delete;
    void operator=(Deduper const&) = delete;

    static Deduper& get_instance()
    {
        static Deduper instance; // Guaranteed to be destroyed.
                              // Instantiated on first use.
        return instance;
    }



    /*
     *   TRIGGER CLASSES
     */

    class Trigger 
    {
    public:
        Trigger() {};
        virtual bool trigger() = 0;
        virtual void write_event() {};
    };

    class AlwaysTrigger : public Trigger 
    {
    public:
        bool trigger() {
            return true;
        }
    };

    class OSDWriteIdleTrigger : public Trigger 
    {
    public:
        std::chrono::steady_clock::time_point last, now;

        bool trigger() {
            now = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::milliseconds> (now - last).count() >= 2000 )
                return true;
            return false;
        }

        void write_event() 
        {
            last = std::chrono::steady_clock::now();
        };

    };


    Trigger* trigger;

};
