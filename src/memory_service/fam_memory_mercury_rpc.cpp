#include "memory_service/fam_memory_mercury_rpc.h"
#include "common/atomic_queue.h"

namespace openfam {

Fam_Memory_Service_Direct *Fam_Memory_Mercury_RPC::memoryService = NULL;
hg_thread_pool_t *Fam_Memory_Mercury_RPC::thread_pool;
Fam_Memory_Mercury_RPC::Fam_Memory_Mercury_RPC(
    const char *name, hg_thread_pool_t *tp, const char *libfabricPort,
    const char *libfabricProvider, const char *fam_path, bool isSharedMemory) {
    Fam_Memory_Mercury_RPC::memoryService = new Fam_Memory_Service_Direct(name, libfabricPort,libfabricProvider,fam_path,isSharedMemory);
    Fam_Memory_Mercury_RPC::thread_pool = tp;
    // int ret = hg_thread_pool_init(1, &Fam_Memory_Mercury_RPC::thread_pool);
    // std::cout<<"Thread pool after init
    // "<<Fam_Memory_Mercury_RPC::thread_pool->sleeping_worker_count<<std::endl;
}
Fam_Memory_Service_Direct *Fam_Memory_Mercury_RPC::get_memory_service() {
    return Fam_Memory_Mercury_RPC::memoryService;
}
#if 0
hg_id_t Fam_Memory_Mercury_RPC::register_with_mercury_fam_aggregation_nowrapper(void)
{
    hg_class_t *hg_class=NULL;
    hg_id_t tmp;

    if(!hg_class) {
        hg_class = hg_engine_get_class();
        cout << "HG class is empty hence creating new" << endl;
    }

    tmp =
        MERCURY_REGISTER(hg_class, "memory_server_aggregation",
                         agg_flush_rpc_in_t, agg_flush_rpc_out_t,
                         Fam_Memory_Mercury_RPC::fam_memory_server_aggregation);

    return (tmp);
}
#endif

hg_id_t Fam_Memory_Mercury_RPC::register_with_mercury_fam_aggregation(
    hg_class_t *hg_class) {
    // hg_class_t *hg_class=NULL;
    hg_id_t tmp;

    if (!hg_class) {
        hg_class = hg_engine_get_class();
        cout << "HG class is empty hence creating new" << endl;
    }

    // std::cout<<"Thread pool after register
    // "<<Fam_Memory_Mercury_RPC::thread_pool->sleeping_worker_count<<std::endl;
    tmp = MERCURY_REGISTER(
        hg_class, "memory_server_aggregation", agg_flush_rpc_in_t,
        agg_flush_rpc_out_t,
        Fam_Memory_Mercury_RPC::fam_memory_server_aggregation_wrapper);

    return (tmp);
}

/* callback triggered upon completion of bulk transfer */
static hg_return_t my_rpc_handler_bulk_cb(const struct hg_cb_info *info) {
    ostringstream message;
    // std::cout<<"**In cb of bulk transfer"<<std::endl;
    Merc_RPC_State *rpcState = (Merc_RPC_State *)info->arg;

    assert(info->ret == HG_SUCCESS);
    // cout << "In callback: " << info->ret<<endl;
    if (info->ret == HG_SUCCESS) {
        pthread_mutex_lock(&rpcState->doneMutex);
        rpcState->done = true;
        pthread_cond_signal(&rpcState->doneCond);
        pthread_mutex_unlock(&rpcState->doneMutex);
    }
    return HG_SUCCESS;
}

#if 0
static HG_THREAD_RETURN_TYPE
myfunc(void *args)
{
    hg_thread_ret_t ret = 0;
    (void) args;
    std::cout<<"myfunc....."<<std::endl;
    /* printf("%d\n", ncalls); */

    return ret;
}
#endif

hg_return_t Fam_Memory_Mercury_RPC::fam_memory_server_aggregation_wrapper(
    hg_handle_t handle) {
    // cout << "Entering fam_memory_server_aggregation _ Server Wrapper " <<
    // endl;
    hg_thread_work *work = new hg_thread_work();
    work->func =
        (hg_thread_func_t)Fam_Memory_Mercury_RPC::fam_memory_server_aggregation;
    work->args = handle;
    // work->func=myfunc;
    // work->args=&handle;
    // std::cout<<"Thread pool before posting
    // "<<Fam_Memory_Mercury_RPC::thread_pool->sleeping_worker_count<<std::endl;
    int ret = hg_thread_pool_post(Fam_Memory_Mercury_RPC::thread_pool, work);
    if (ret != HG_SUCCESS) {
        std::cout << "Thread pool post failed with " << ret << std::endl;
        return (hg_return_t)ret;
    }
    // std::cout<<"Thread pool after posting
    // "<<Fam_Memory_Mercury_RPC::thread_pool->sleeping_worker_count<<std::endl;
    // cout << "Posted work"<<ret<<std::endl;
    return HG_SUCCESS;
}

hg_return_t Fam_Memory_Mercury_RPC::fam_memory_server_aggregation(hg_handle_t handle) {
    // cout << "Entering fam_memory_server_aggregation _ Server Side " << endl;
    agg_flush_rpc_in_t request;
    hg_return_t ret;
    hg_bulk_t bulk_buffer;
    const struct hg_info *hgi;
    Merc_RPC_State *rpcState_buffer = new Merc_RPC_State();
    rpcState_buffer->done = false;
    rpcState_buffer->doneCond = PTHREAD_COND_INITIALIZER;
    rpcState_buffer->doneMutex = PTHREAD_MUTEX_INITIALIZER;

    Merc_RPC_State *rpcState_index = new Merc_RPC_State();
    rpcState_index->done = false;
    rpcState_index->doneCond = PTHREAD_COND_INITIALIZER;
    rpcState_index->doneMutex = PTHREAD_MUTEX_INITIALIZER;

    // Fam_Memory_Service_Direct *m1 = get_memory_service();

    ret = HG_Get_input(handle, &request);
    assert(ret == HG_SUCCESS);
    my_rpc_out_t response;

    // allocate fam data item for buffer and offset
    // std::cout << "Bulk buffer in server : " << request.bulk_buffer
    //          << std::endl;
    hgi = HG_Get_info(handle);
    hg_size_t size = (hg_size_t)request.elementsize * request.nelements;

    // allocate fam data item for buffer and offset
    Fam_Region_Item_Info buffer_info;
    buffer_info =
        Fam_Memory_Mercury_RPC::memoryService->allocate(ATOMIC_REGION_ID, size);
    // create local target buffer for bulk transfer
    void *buffer = Fam_Memory_Mercury_RPC::memoryService->get_local_pointer(
        ATOMIC_REGION_ID, buffer_info.offset);
    // std::cout << "buffer is at :" << buffer << std::endl;

    ret = HG_Bulk_create(hgi->hg_class, 1, &buffer, &size, HG_BULK_WRITE_ONLY,
                         &bulk_buffer);

    // std::cout << "Transfering..... " << std::endl;
    // Do bulk transfer
    ret =
        HG_Bulk_transfer(hgi->context, my_rpc_handler_bulk_cb, rpcState_buffer,
                         HG_BULK_PULL, hgi->addr, request.bulk_buffer, 0,
                         bulk_buffer, 0, size, HG_OP_ID_IGNORE);

    hg_bulk_t bulk_offset;
    // Bulk transfer offset
    hg_size_t offset_size = (hg_size_t)sizeof(uint64_t) * request.nelements;
    Fam_Region_Item_Info offset_info;
    offset_info = Fam_Memory_Mercury_RPC::memoryService->allocate(
        ATOMIC_REGION_ID, offset_size);
    // std::cout << "allocated :" << offset_info.base << " "
    //          << offset_info.offset << std::endl;
    void *offset = Fam_Memory_Mercury_RPC::memoryService->get_local_pointer(
        ATOMIC_REGION_ID, offset_info.offset);

    /* First I/O Completion */
    // std::cout<<"Checking for IO completion"<<std::endl;
    pthread_mutex_lock(&rpcState_buffer->doneMutex);
    // std::cout<<"Checking for IO completion took mutex"<<std::endl;
    while (!rpcState_buffer->done) {
        // sched_yield();
        /*
        struct timespec time;
        timespec_get(&time, TIME_UTC);
        time.tv_sec += 2;
        */
        // std::cout<<"Checking conditional var"<<std::endl;
        pthread_cond_wait(&rpcState_buffer->doneCond,
                          &rpcState_buffer->doneMutex);
        // std::cout<<"Checked conditional var"<<std::endl;
    }
    pthread_mutex_unlock(&rpcState_buffer->doneMutex);
    // std::cout<<"Checking for IO completion 2"<<std::endl;
    sched_yield();
    /* Do second IO */
#if 1
    ret = HG_Bulk_create(hgi->hg_class, 1, &offset, &offset_size,
                         HG_BULK_WRITE_ONLY, &bulk_offset);
    // Do bulk transfer
    ret = HG_Bulk_transfer(hgi->context, my_rpc_handler_bulk_cb, rpcState_index,
                           HG_BULK_PULL, hgi->addr, request.bulk_offset, 0,
                           bulk_offset, 0, offset_size, HG_OP_ID_IGNORE);
#endif
    sched_yield();
    // sleep(10);
    // sched_yield();
#if 0
		std::cout<<"Printing data buffer:"<<std::endl;
		for(uint32_t i=0;i<size/sizeof(int);i++) {
          		std::cout<<*((uint64_t*)offset+i)<<" : "<<*((int*)buffer+i)<<"  ";
		}	
		std::cout<<std::endl;
#endif
#if 1

    pthread_mutex_lock(&rpcState_index->doneMutex);
    while (!rpcState_index->done)
        pthread_cond_wait(&rpcState_index->doneCond,
                          &rpcState_index->doneMutex);
    pthread_mutex_unlock(&rpcState_index->doneMutex);
#endif
    // bool result;
    try {
        Fam_Memory_Mercury_RPC::memoryService->aggregate_indexed_add(
            request.region_id, request.offset, request.opcode,
            request.elementsize, request.nelements, buffer_info.offset,
            offset_info.offset);
    } catch (Fam_Exception &e) {
        response.errorcode = e.fam_error();
        response.errormsg = strdup(e.fam_error_msg());
    }
    response.size = 123;
    response.errorcode = 0;
    // cout << "Exiting from fam_memory_server_aggregation _ Server Side " <<
    // endl;
    ret = HG_Respond(handle, NULL, NULL, &response);
    assert(ret == HG_SUCCESS);
    (void)ret;
    HG_Destroy(handle);
    return ret;
}
}
