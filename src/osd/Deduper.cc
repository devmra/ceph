#include "Deduper.h"
#include <regex>
#include "DedupLogger.h"
#include "PG.h"
#include <unistd.h>
#include "common/Clock.h"
#include <chrono>

#define POST_PROCESS_TIME_MS 0
#define SKIP_DEDUP_WRITE 0
#define DISABLE_DEDUP 0


static std::regex rgx("([[:alnum:]]*)_(\\d*)_([[:alpha:]]*)_");


Deduper::Deduper() 
    : waiting_lock("Deduper::waiting_lock"),
    cache_lock("Deduper::cache_lock"),
    skip_lock("Deduper::skip_lock"),
    q_lock("Deduper::q_lock"),
    waiting_ops2(256)
{
    int ret = rados.init("admin");
    if (ret < 0) 
        dlog("!!! !!! !!! !!! !!!  RADOS INIT ERROR\n");

    const char* argv[] = {"deduper", "-c", "/etc/ceph/ceph.conf" };
    ret = rados.conf_parse_argv(3, argv);
    if (ret < 0) 
        dlog("!!! !!! !!! !!! !!!  RADOS CONF PARSE ERROR\n");

    ret = rados.conf_read_file("/etc/ceph/ceph.conf");
    if (ret < 0) 
        dlog("!!! !!! !!! !!! !!!  RADOS CONF READ ERROR\n");

    ret = rados.connect();
    if (ret < 0)
        dlog("!!! !!! !!! !!! !!!  RADOS CONNECT ERROR\n");

    time_stats_init_named (&rd_data, "read_data_read");
    time_stats_init_named (&rd_chunks, "read_chunks_read");
    time_stats_init_named (&rd_concat_cache, "read_concat_cache");
    time_stats_init_named (&rd_bl, "read_bl_assign");

    time_stats_init_named (&wt_read, "write_read_data");
    time_stats_init_named (&wt_chunks, "write_chunk_writes");
    time_stats_init_named (&wt_recipe, "write_recipe_write");

    time_stats_init_named (&filter_oh, "op_filter_overhead");
    time_stats_init_named (&ceph_oh, "ceph_overhead");

    time_stats_init_named (&reg_rd, "regular_read");
    time_stats_init_named (&reg_write, "regular_write");

    //trigger = new OSDWriteIdleTrigger();
    trigger = new AlwaysTrigger();
}

void Deduper::reset_stats()
{
    dlog("\n\n***RESET\n");
    time_stats_destroy (&rd_data);
    time_stats_destroy (&rd_chunks);
    time_stats_destroy (&rd_concat_cache);
    time_stats_destroy (&rd_bl);

    time_stats_destroy (&wt_read);
    time_stats_destroy (&wt_chunks);
    time_stats_destroy (&wt_recipe);

    time_stats_destroy (&reg_rd);
    time_stats_destroy (&reg_write);


    time_stats_init_named (&rd_data, "read_data_read");
    time_stats_init_named (&rd_chunks, "read_chunks_read");
    time_stats_init_named (&rd_concat_cache, "read_concat_cache");
    time_stats_init_named (&rd_bl, "read_bl_assign");

    time_stats_init_named (&wt_read, "write_read_data");
    time_stats_init_named (&wt_chunks, "write_chunk_writes");
    time_stats_init_named (&wt_recipe, "write_recipe_write");

    time_stats_init_named (&reg_rd, "regular_read");
    time_stats_init_named (&reg_write, "regular_write");
}

void Deduper::flush_stats()
{
#if PRINT_STATS
    char st[1024*8];
    strcpy(st, "\n#################################################\n");

    time_stats_sprint(st+strlen(st), &rd_data);
    time_stats_sprint(st+strlen(st), &rd_chunks);
    time_stats_sprint(st+strlen(st), &rd_concat_cache);
    time_stats_sprint(st+strlen(st), &rd_bl);

    time_stats_sprint(st+strlen(st), &wt_read);
    time_stats_sprint(st+strlen(st), &wt_chunks);
    time_stats_sprint(st+strlen(st), &wt_recipe);
    
    time_stats_sprint(st+strlen(st), &filter_oh);
    time_stats_sprint(st+strlen(st), &ceph_oh);

    time_stats_sprint(st+strlen(st), &reg_rd);
    time_stats_sprint(st+strlen(st), &reg_write);
    idlog(st);
#endif
}

bool Deduper::filter_op (PGRef pg, OpRequestRef& op)
{
    #if DISABLE_DEDUP
        //return 0;
    #endif


    Message* msg = op->get_req();
    MOSDOp *m = static_cast<MOSDOp*>(op->get_req());

    //not a regular op, return false
    if (m->get_type() != CEPH_MSG_OSD_OP)
        return false;

    m->finish_decode();
    bool skip;
    std::string name = m->get_oid().name;

    std::smatch match;
    std::regex_search(name, match, rgx);

    //check if its a write to update our trigger
    if (m->ops[0].op.op == CEPH_OSD_OP_WRITEFULL)
            trigger->write_event();

    //if its not dedup, we return false
    if (match.empty()) {
        //dlog("Op is not dedup, so continue\n");
        return false;
    }

    #if PRINT_STATS
        utime_t elaps = ceph_clock_now(pg->get_cct());
        elaps -= msg->get_recv_stamp();
        dlog("\n\n***** ELAPS: %.6f s\n", elaps.to_nsec()/1000000000.0);
        time_stats_stop_dummy(&Deduper::get_instance().ceph_oh, elaps.to_nsec()/1000000000.0);
        time_stats_start(&Deduper::get_instance().filter_oh);
    #endif
    
    bool off = match.str(3) == "off" ? true : false;
    uint32_t chunk_sz = std::stoul(match.str(2));

    DedupOpBase* dop;
    switch (m->ops[0].op.op) { 
        case CEPH_OSD_OP_WRITEFULL:
            //if its in our list, its a recipe write so we skip and let ceph do it
            if (check_skip_write(name)) {
                //dlog("\n\n &&&&&&&&&&&&&&&&&&&& ITS  RECIPE, SKIP\n\n");
                return false;
            }

            dop = new DedupOpPostProcessWrite(pg, op, POST_PROCESS_TIME_MS);
            skip = false;
        break;

        case CEPH_OSD_OP_READ:
            if (m->ops[0].op.extent.offset == 0) {
                dlog ("*****Offset == 0 so we skip\n");
                return false;
            }

            dlog("************** It's an actual dedup read\n");
            m->ops[0].op.extent.offset = 0;
            dop = new DedupOpRead(pg, op);
            skip = true;
        break;

        default:
            return false;
    }

    dop->set_data(name, chunk_sz, &rados);

    {
        Mutex::Locker lock(q_lock);
        waiting_ops2.enqueue(dop);
    }

    #if PRINT_STATS
        time_stats_stop(&Deduper::get_instance().filter_oh);
    #endif

    return skip;
}

void Deduper::skip_write(std::string name)
{
    Mutex::Locker lock(skip_lock);
    skip_writes.insert(name);
}

bool Deduper::check_skip_write(std::string name)
{
    std::set<std::string>::iterator it;

    Mutex::Locker lock(skip_lock);
    it = skip_writes.find(name);

    if (it != skip_writes.end()) {
        skip_writes.erase (it);
        return true;
    }
    return false;

}

void Deduper::cache_read (std::string name, librados::bufferlist* bl)
{
    Mutex::Locker lock(cache_lock);
    cached_reads[name] = new librados::bufferlist(*bl);
}

librados::bufferlist* Deduper::get_cached_read (std::string name)
{
    try {
        Mutex::Locker lock(cache_lock);
        librados::bufferlist* c = cached_reads.at(name);
        cached_reads.erase(name);
        return c;
    }
    catch(...) {
        return 0;
    }
}


//thread entry point
void* Deduper::run(void* arg)
{
    dlog(">>> Dedup Thread Running\n");
    Deduper& me = Deduper::get_instance();
    
    while(1) {
        //check waiting ops and issue the ones that are ready
        DedupOpBase* op = 0;
        DedupOpBase** op2 = 0;
        op2 = me.waiting_ops2.peek();

        //check if the op in head is ready
        if (op2 != NULL && (*op2)->is_ready() ) {
            op = *op2;
            
            dlog(">>> op ready\n");
            //op is ready, but we need to check if we can trigger the entire op
            bool dedup_trigger = me.trigger->trigger();

            if (!dedup_trigger) {
                //nope, we leave
                op = 0;
            }
            else {
                //op is ready, trigger is true, remove from queue
                me.waiting_ops2.pop();
                dlog(">>> trigger is true\n");
                //if true this will simply skip all dedup write.
                //since it was written before, it will never "trigger" the dedup
                #if SKIP_DEDUP_WRITE
                    if (op->is_write()) {
                        delete op;
                        op = 0;
                    }
                #endif
            }
        }
        
        if (op) {
            dlog("Trigger is true and op is ready. Name: %.20s\n", op->name.c_str());
            me.issued_ops.push_back(op);
        }

        for (std::list<DedupOpBase*>::iterator it = me.issued_ops.begin() 
                    ; it != me.issued_ops.end() ;)
        {
            if ( (*it)->is_done() ) {
                //TODO: might need lock
                me.flush_stats();
                dlog ("Operation is done: %.20s\n", (*it)->name.c_str() );
                //requeue to OSD. if it's a write we'll write the recipe, if read
                //read is cached and will return the entire object
                (*it)->queue_back();
                delete *it; 
                it = me.issued_ops.erase(it);
            }
            else
                ++it;
        }
        //usleep(50);
    }
}