#include "Deduper.h"
#include "DedupLogger.h"
#include "DedupOp.h"

/************************
 * 
 * DedupRadosOps
 * 
 ***********************/

DedupRadosOps::DedupRadosOps(librados::Rados* rados) {
    int ret = rados->ioctx_create("ded1", io_ctx);
    if (ret < 0) 
        dlog("!!! !!! !!! !!! !!!  RADOS IOCTX ERROR\n");

    dlog("DedupRadosOps constructor created ioctx  pool -> ");
    dlog("%.20s\n", io_ctx.get_pool_name().c_str());

    this->error = 0;
}

void DedupRadosOps::create_ctx(librados::Rados* rados) {
    int ret = rados->ioctx_create("ded1", io_ctx);
    if (ret < 0) 
        dlog("!!! !!! !!! !!! !!!  RADOS IOCTX ERROR\n");

    dlog("DedupRadosOps constructor created ioctx  pool -> ");
    dlog("%.20s\n", io_ctx.get_pool_name().c_str());
}


DedupRadosOps::~DedupRadosOps()
{
    for (std::list <librados::bufferlist*>::iterator it = bufs.begin() 
                 ; it != bufs.end() ; ++it) {
        delete *it;
    }
    bufs.clear();
}


int DedupRadosOps::get_return_value()
{
    return this->error;
}

bool DedupRadosOps::is_complete() 
{
    for (std::list <librados::AioCompletion*>::iterator it = comps.begin() 
            ; it != comps.end() ; )
    {
        //if ( (*it)->is_complete() ) {
        if ( (*it)->is_safe() ) {
            dlog("One Op is done, %d left\n", comps.size()-1);

            int er = (*it)->get_return_value();
            this->error = er ? er : this->error;

            delete *it;
            it = comps.erase(it);
        }
        else
            ++it;
    }

    if (comps.empty()) {
        dlog("All done\n");
        comps.clear();
        return true;
    }
    return false;
}


std::string DedupRadosOps::murmur_hash(char* buf, uint32_t len)
{
  uint64_t h[2];
  MurmurHash3_x64_128(buf, len, 97, h);
  std::string hash = std::to_string(h[0]).append(std::to_string(h[1]));
  if (hash.length() > 200)
    dlog("\n############# HASH IS TOO LONG\n\n");
  return hash;
}

void DedupRadosOps::issue_one_full_read(std::string name, librados::bufferlist* bl)
{
    librados::AioCompletion *c = librados::Rados::aio_create_completion();
    dlog("DedupRadosOps::issue_one_full_read   issuing read %s\n", name.c_str());
    int ret = io_ctx.aio_read(name, c, bl, 0, 0);
    if (ret < 0) 
        dlog("!!! !!! !!! !!! !!!  issue_one_full_read AIO READ ERROR\n");
    comps.push_back(c);
    dlog("DedupRadosOps::issue_one_full_read   issued\n");
}

void DedupRadosOps::issue_multiple_hashed_writes(librados::bufferlist* bl, 
        uint32_t chunk_size)
{
    uint32_t num_writes = (bl->length() + chunk_size - 1) / chunk_size;
    std::string recipe = "0xdeduprecipe";
    int ret;

    dlog("DedupRadosOps::issue_multiple_hashed_writes issuing %d writes\n", num_writes);
    dlog("Total %d  chunk %d\n", bl->length(), chunk_size);

    char* buf = bl->get_contiguous(0, bl->length());

    for (uint32_t i = 0, j = 0 ; i < bl->length() ; i += chunk_size, j++) {
        uint32_t bytes_to_write = 
                bl->length()-i > chunk_size ? 
                chunk_size : bl->length()-i;

        librados::bufferlist* chunk = new librados::bufferlist();
        
        //chunk->substr_of(*bl, i, bytes_to_write);
        chunk->append(buf+i, bytes_to_write);

        std::string hash = murmur_hash(chunk->c_str(), bytes_to_write);
        recipe.append(hash);
        //if not last write, append comma
        if (i+chunk_size < bl->length())
            recipe.append(",");

        //dlog("Chunk %d/%d   hash %s  len %d bytes\n", j+1, num_writes, 
        //            hash.c_str(), chunk->length());

        librados::AioCompletion* c = librados::Rados::aio_create_completion();
        
        ret = io_ctx.aio_write_full(hash, c, *chunk);
        if (ret != 0)
            dlog("!!! !!! !!! !!! !!!  AIO_WRITE ERROR\n");

        bufs.push_back(chunk);
        comps.push_back(c);
    }

    dlog("All writes issued\n");
    bl->clear();
    bl->append(recipe);
}

void DedupRadosOps::issue_multiple_split_reads(std::string recipe)
{
    std::vector<std::string> chunk_names;

    recipe = recipe.substr(13, recipe.length()-13);

    boost::split (chunk_names, recipe, boost::is_any_of(","));  
    //int ret;

    for (uint32_t i = 0 ; i < chunk_names.size() ; i++) {
        librados::bufferlist* bl = new librados::bufferlist();
        librados::AioCompletion* c = librados::Rados::aio_create_completion();
        
        //dlog("Issuing read %d/%d\n", i+1, chunk_names.size());
        //int ret = io_ctx.aio_write(chunk_names[i], c, const_cast<const librados::bufferlist&>(*bl), 0, 0);
        int ret = io_ctx.aio_read(chunk_names[i], c, bl, 0, 0);
        if (ret != 0)
            dlog("READ ERROR\n");

        comps.push_back(c);
        bufs.push_back(bl);
    }
    dlog("bufs has %d elements\n", bufs.size());
}

void DedupRadosOps::issue_single_write (std::string name, librados::bufferlist* bl) 
{
    librados::AioCompletion *c = librados::Rados::aio_create_completion();
    //dlog("DedupRadosOps::issue_single_write   issuing recipe write\n");
    int ret = io_ctx.aio_write_full(name, c, *bl);
    if (ret < 0) 
        ;//dlog("!!! !!! !!! !!! !!!  issue_single_write AIO WRITE ERROR\n");
    comps.push_back(c);
}


void DedupRadosOps::concatenate_bufs(librados::bufferlist* bl)
{
    dlog("inside concat, %d bufs\n", bufs.size());
    for (auto& it : bufs) {
        bl->append (*it);
        dlog("appended %d bytes\n", it->length());
    }

    for (std::list <librados::bufferlist*>::iterator it = bufs.begin() 
                    ; it != bufs.end() ; ++it) {
        delete *it; 
    }
    bufs.clear();
}


/************************
 * 
 * DedupOpBase
 * 
 ***********************/

DedupOpBase::DedupOpBase (spg_t& pg, OpRequestRef& op, uint32_t time)
{
    this->op = op;
    this->pg = pg;
    this->dedup_ops = 0;
    this->state = START;
    this->timer = std::chrono::system_clock::now();
    std::chrono::milliseconds mss(time);
    this->timer += mss;
}

DedupOpBase::~DedupOpBase() 
{
    //if (dedup_ops)
    //    delete dedup_ops;
}

bool DedupOpBase::is_ready() 
{
    std::chrono::time_point<std::chrono::system_clock> now = std::chrono::system_clock::now();
    return this->timer < now ? true : false;
}

void DedupOpBase::set_data(std::string name, uint32_t chunk_sz, librados::Rados* rados)
{
    this->name = name;
    this->chunk_sz = chunk_sz;
    this->rados = rados;
}


/************************
 * 
 * DedupOpPostProcessWrite
 * 
 ***********************/

void DedupOpPostProcessWrite::execute() {
    //dlog("DedupWrite execute\n");
    switch (state) {
        case WAIT_WRITING_SLEEP:
            {
                this->timer = std::chrono::system_clock::now();
                std::chrono::milliseconds mss(10);
                this->timer += mss;
                state = WAIT_WRITING_SLEEPING;
            }
        break;

        case WAIT_WRITING_SLEEPING:
            state = is_ready() ? WRITING : WAIT_WRITING_SLEEPING;
        break;

        case SLEEP:
            {
                this->timer = std::chrono::system_clock::now();
                std::chrono::milliseconds mss(5000);
                this->timer += mss;
                state = SLEEPING;
            }
        break;

        case SLEEPING:
            state = is_ready() ? START : SLEEPING;
        break;

        case START:
            #if PRINT_STATS
                time_stats_start(&Deduper::get_instance().wt_read);
            #endif

            dlog("DedupWrite START  %s\n", name.c_str());
            dedup_ops = new DedupRadosOps (this->rados);
            data.clear();

            //read non dedup'd data
            dedup_ops->issue_one_full_read (name, &data);
            state = READING;
        break;

        case READING:
            if (dedup_ops->is_complete()) {
                dlog("DedupWrite READ COMPLETE");
                //if not enough data or read failed, redo
                if (dedup_ops->get_return_value() < 0 ||  data.length() <= 13) {
                    dlog("################### Return: %d  Too little data:  %d.. redo#####\n"
                        , dedup_ops->get_return_value(), data.length());

                    dlog("Content: %.100s\n", data.c_str());
                    delete dedup_ops;
                    state = SLEEP;
                    return;
                }

                #if PRINT_STATS
                    time_stats_stop(&Deduper::get_instance().wt_read);   
                    time_stats_start(&Deduper::get_instance().wt_chunks);
                #endif

                dlog("DedupWrite READ data, now issuing writes, read %d bytes, name %s\n", data.length(), name.c_str());
                delete dedup_ops;
                dedup_ops = new DedupRadosOps (this->rados);
                //chunk data and issue all writes
                dedup_ops->issue_multiple_hashed_writes (&data, chunk_sz);
                state = WRITING;
            }
        break;

        case WRITING:
            if (dedup_ops->is_complete()) {
                #if PRINT_STATS
                    time_stats_stop(&Deduper::get_instance().wt_chunks);   
                    time_stats_start(&Deduper::get_instance().wt_recipe);
                #endif

                dlog("DedupWrite CHUNK WRITING complete  %s\n", name.c_str());
                delete dedup_ops;
                //we'll write the recipe, so we have to skip deduplication
                dedup_ops = new DedupRadosOps (this->rados);
                dlog("Issuing recipe write with %d bytes\n", data.length());
                Deduper::get_instance().skip_write(name);
                dedup_ops->issue_single_write (name, &data);
                state = WRITING_RECIPE;
            }
            else {
                //state = WAIT_WRITING_SLEEP;
                //dlog("Still waiting..\n", data.length());
            }
        break;

        case WRITING_RECIPE:
            if (dedup_ops->is_complete()) {
                delete dedup_ops;
                state = DONE;

                dlog("DEDUP WRITE COMPLETE!  %s\n", name.c_str());

                #if PRINT_STATS
                    time_stats_stop(&Deduper::get_instance().wt_recipe);
                #endif
            }
        break;

        default:
            dlog("DedupOpPostProcessWrite UNKNOWN CASE/STATE\n");
        break;
    }
}

bool DedupOpPostProcessWrite::is_done() {
    if (state == DONE)
        return true;
    
    execute();
    return false;
}

void DedupOpPostProcessWrite::queue_back()
{
    ;
}



/************************
 * 
 * DedupOpRead
 * 
 ***********************/

void DedupOpRead::execute() {
    switch (state) {
        case SLEEP:
            {
                this->timer = std::chrono::system_clock::now();
                std::chrono::milliseconds mss(2);
                this->timer += mss;
                state = SLEEPING;
            }
        break;

        case SLEEPING:
            state = is_ready() ? START : SLEEPING;
        break;

        case START:
            //issue recipe read
            dlog("DedupOpRead state START\n");

            #if PRINT_STATS
                time_stats_start(&Deduper::get_instance().rd_data);   
            #endif

            dedup_ops = new DedupRadosOps (rados);
            dedup_ops->issue_one_full_read (name, &data);
            state = READING_RECIPE;
        break;

        case READING_RECIPE:
            if (dedup_ops->is_complete()) {
                
                if (dedup_ops->get_return_value() < 0) {
                    dlog("########################## READ FAILED #################\n", data.length());
                    delete dedup_ops;
                    state = SLEEP;
                    return;
                }
                
                dlog("DedupOpRead state READING_RECIPE read data %d bytes: %.13s\n", data.length(), data.to_str().c_str());
                //if its not a recipe
                //if (data.to_str().rfind("0xdeduprecipe", 0) != 0 ) {
                if ( strncmp(data.to_str().c_str(), "0xdeduprecipe", 13) ) {
                    dlog("DedupOpRead data is not recipe, cache and go\n");
                    Deduper::get_instance().cache_read(name, &data);
                    state = DONE;
                    break;
                }

                #if PRINT_STATS
                    time_stats_stop(&Deduper::get_instance().rd_data);   
                    time_stats_start(&Deduper::get_instance().rd_chunks);
                #endif

                //issue chunk writes
                delete dedup_ops;
                
                dlog("Read recipe, %d bytes long:  %s\n", data.length(), data.c_str());

                dedup_ops = new DedupRadosOps (rados);
                dedup_ops->issue_multiple_split_reads (data.to_str());
                state = READING;
            }
        break;

        case READING:
            if (dedup_ops->is_complete()) {
                #if PRINT_STATS
                    time_stats_stop(&Deduper::get_instance().rd_chunks);   
                    time_stats_start(&Deduper::get_instance().rd_concat_cache);
                #endif


                dlog("DedupOpRead state READING\n");
                data.clear();
                dedup_ops->concatenate_bufs(&data);
                Deduper::get_instance().cache_read(name, &data);
                delete dedup_ops;
                state = DONE;

                #if PRINT_STATS
                    time_stats_stop(&Deduper::get_instance().rd_concat_cache);   
                #endif
            }
        break;

        default:
            dlog("DedupOpRead UNKNOWN CASE/STATE\n");
        break;
    }
}

bool DedupOpRead::is_done() {
    if (state == DONE) {
        dlog("Read done\n");
        return true;
    }
    
    execute();
    return false;
}

void DedupOpRead::queue_back()
{
	// mra: need to check if this is ok. 
    //Mutex::Locker l(pg->queue_lock);
    //pg->queue_op(op);
	dlog("! disabled by MRA. need to recheck.");
}

/*************************************************
 ***
 ***    HASH  \/ \/ \/
 ***
 ***********************************************/

#ifdef __GNUC__
#define FORCE_INLINE __attribute__((always_inline)) inline
#else
#define FORCE_INLINE inline
#endif
static FORCE_INLINE uint32_t rotl32 ( uint32_t x, int8_t r ) {
  return (x << r) | (x >> (32 - r));
}
static FORCE_INLINE uint64_t rotl64 ( uint64_t x, int8_t r ) {
  return (x << r) | (x >> (64 - r));
}
#define	ROTL32(x,y)	rotl32(x,y)
#define ROTL64(x,y)	rotl64(x,y)
#define BIG_CONSTANT(x) (x##LLU)
#define getblock(p, i) (p[i])
static FORCE_INLINE uint32_t fmix32 ( uint32_t h ) {
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;
  return h;
}
static FORCE_INLINE uint64_t fmix64 ( uint64_t k ) {
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;
  return k;
}

void MurmurHash3_x86_32 ( const void * key, int len,
                          uint32_t seed, void * out )
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 4;
  int i;
  uint32_t h1 = seed;
  uint32_t c1 = 0xcc9e2d51;
  uint32_t c2 = 0x1b873593;
  //----------
  // body
  const uint32_t * blocks = (const uint32_t *)(data + nblocks*4);
  for(i = -nblocks; i; i++)
  {
    uint32_t k1 = getblock(blocks,i);
    k1 *= c1;
    k1 = ROTL32(k1,15);
    k1 *= c2;
    h1 ^= k1;
    h1 = ROTL32(h1,13); 
    h1 = h1*5+0xe6546b64;
  }
  //----------
  // tail
  const uint8_t * tail = (const uint8_t*)(data + nblocks*4);
  uint32_t k1 = 0;
  switch(len & 3)
  {
  case 3: k1 ^= tail[2] << 16;
  case 2: k1 ^= tail[1] << 8;
  case 1: k1 ^= tail[0];
          k1 *= c1; k1 = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
  };
  //----------
  // finalization
  h1 ^= len;
  h1 = fmix32(h1);
  *(uint32_t*)out = h1;
} 

void MurmurHash3_x86_128 ( const void * key, const int len,
                           uint32_t seed, void * out )
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 16;
  int i;
  uint32_t h1 = seed;
  uint32_t h2 = seed;
  uint32_t h3 = seed;
  uint32_t h4 = seed;
  uint32_t c1 = 0x239b961b; 
  uint32_t c2 = 0xab0e9789;
  uint32_t c3 = 0x38b34ae5; 
  uint32_t c4 = 0xa1e38b93;
  //----------
  // body
  const uint32_t * blocks = (const uint32_t *)(data + nblocks*16);
  for(i = -nblocks; i; i++) {
    uint32_t k1 = getblock(blocks,i*4+0);
    uint32_t k2 = getblock(blocks,i*4+1);
    uint32_t k3 = getblock(blocks,i*4+2);
    uint32_t k4 = getblock(blocks,i*4+3);
    k1 *= c1; k1  = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
    h1 = ROTL32(h1,19); h1 += h2; h1 = h1*5+0x561ccd1b;
    k2 *= c2; k2  = ROTL32(k2,16); k2 *= c3; h2 ^= k2;
    h2 = ROTL32(h2,17); h2 += h3; h2 = h2*5+0x0bcaa747;
    k3 *= c3; k3  = ROTL32(k3,17); k3 *= c4; h3 ^= k3;
    h3 = ROTL32(h3,15); h3 += h4; h3 = h3*5+0x96cd1c35;
    k4 *= c4; k4  = ROTL32(k4,18); k4 *= c1; h4 ^= k4;
    h4 = ROTL32(h4,13); h4 += h1; h4 = h4*5+0x32ac3b17;
  }
  //----------
  // tail
  const uint8_t * tail = (const uint8_t*)(data + nblocks*16);
  uint32_t k1 = 0;
  uint32_t k2 = 0;
  uint32_t k3 = 0;
  uint32_t k4 = 0;
  switch(len & 15)
  {
  case 15: k4 ^= tail[14] << 16;
  case 14: k4 ^= tail[13] << 8;
  case 13: k4 ^= tail[12] << 0;
           k4 *= c4; k4  = ROTL32(k4,18); k4 *= c1; h4 ^= k4;
  case 12: k3 ^= tail[11] << 24;
  case 11: k3 ^= tail[10] << 16;
  case 10: k3 ^= tail[ 9] << 8;
  case  9: k3 ^= tail[ 8] << 0;
           k3 *= c3; k3  = ROTL32(k3,17); k3 *= c4; h3 ^= k3;
  case  8: k2 ^= tail[ 7] << 24;
  case  7: k2 ^= tail[ 6] << 16;
  case  6: k2 ^= tail[ 5] << 8;
  case  5: k2 ^= tail[ 4] << 0;
           k2 *= c2; k2  = ROTL32(k2,16); k2 *= c3; h2 ^= k2;
  case  4: k1 ^= tail[ 3] << 24;
  case  3: k1 ^= tail[ 2] << 16;
  case  2: k1 ^= tail[ 1] << 8;
  case  1: k1 ^= tail[ 0] << 0;
           k1 *= c1; k1  = ROTL32(k1,15); k1 *= c2; h1 ^= k1;
  };
  //----------
  // finalization
  h1 ^= len; h2 ^= len; h3 ^= len; h4 ^= len;
  h1 += h2; h1 += h3; h1 += h4;
  h2 += h1; h3 += h1; h4 += h1;
  h1 = fmix32(h1);
  h2 = fmix32(h2);
  h3 = fmix32(h3);
  h4 = fmix32(h4);
  h1 += h2; h1 += h3; h1 += h4;
  h2 += h1; h3 += h1; h4 += h1;
  ((uint32_t*)out)[0] = h1;
  ((uint32_t*)out)[1] = h2;
  ((uint32_t*)out)[2] = h3;
  ((uint32_t*)out)[3] = h4;
}

void MurmurHash3_x64_128 ( const void * key, const int len,
                           const uint32_t seed, void * out )
{
  const uint8_t * data = (const uint8_t*)key;
  const int nblocks = len / 16;
  int i;
  uint64_t h1 = seed;
  uint64_t h2 = seed;
  uint64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
  uint64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

  //----------
  // body
  const uint64_t * blocks = (const uint64_t *)(data);
  for(i = 0; i < nblocks; i++)
  {
    uint64_t k1 = getblock(blocks,i*2+0);
    uint64_t k2 = getblock(blocks,i*2+1);
    k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;
    h1 = ROTL64(h1,27); h1 += h2; h1 = h1*5+0x52dce729;
    k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;
    h2 = ROTL64(h2,31); h2 += h1; h2 = h2*5+0x38495ab5;
  }

  //----------
  // tail
  const uint8_t * tail = (const uint8_t*)(data + nblocks*16);
  uint64_t k1 = 0;
  uint64_t k2 = 0;
  switch(len & 15)
  {
  case 15: k2 ^= (uint64_t)(tail[14]) << 48;
  case 14: k2 ^= (uint64_t)(tail[13]) << 40;
  case 13: k2 ^= (uint64_t)(tail[12]) << 32;
  case 12: k2 ^= (uint64_t)(tail[11]) << 24;
  case 11: k2 ^= (uint64_t)(tail[10]) << 16;
  case 10: k2 ^= (uint64_t)(tail[ 9]) << 8;
  case  9: k2 ^= (uint64_t)(tail[ 8]) << 0;
           k2 *= c2; k2  = ROTL64(k2,33); k2 *= c1; h2 ^= k2;
  case  8: k1 ^= (uint64_t)(tail[ 7]) << 56;
  case  7: k1 ^= (uint64_t)(tail[ 6]) << 48;
  case  6: k1 ^= (uint64_t)(tail[ 5]) << 40;
  case  5: k1 ^= (uint64_t)(tail[ 4]) << 32;
  case  4: k1 ^= (uint64_t)(tail[ 3]) << 24;
  case  3: k1 ^= (uint64_t)(tail[ 2]) << 16;
  case  2: k1 ^= (uint64_t)(tail[ 1]) << 8;
  case  1: k1 ^= (uint64_t)(tail[ 0]) << 0;
           k1 *= c1; k1  = ROTL64(k1,31); k1 *= c2; h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len; h2 ^= len;
  h1 += h2;
  h2 += h1;
  h1 = fmix64(h1);
  h2 = fmix64(h2);
  h1 += h2;
  h2 += h1;
  ((uint64_t*)out)[0] = h1;
  ((uint64_t*)out)[1] = h2;
}
