#include "memory_service/fam_memory_mercury_rpc.h"
#include "common/atomic_queue.h"

namespace openfam {

Fam_Memory_Service_Direct *Fam_Memory_Mercury_RPC::memoryService = NULL;
Fam_Memory_Mercury_RPC::Fam_Memory_Mercury_RPC(const char *name,
                              const char *libfabricPort,
                              const char *libfabricProvider,
                              const char *fam_path,
                              bool isSharedMemory) {
    Fam_Memory_Mercury_RPC::memoryService = new Fam_Memory_Service_Direct(name, libfabricPort,libfabricProvider,fam_path,isSharedMemory);
}
Fam_Memory_Service_Direct *Fam_Memory_Mercury_RPC::get_memory_service() {
    return Fam_Memory_Mercury_RPC::memoryService;
}

hg_id_t Fam_Memory_Mercury_RPC::register_with_mercury_fam_aggregation(void)
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

/* callback triggered upon completion of bulk transfer */
static hg_return_t my_rpc_handler_bulk_cb(const struct hg_cb_info *info) {
    std::cout << "Bulk transfer completed" << std::endl;
    return (hg_return_t)(0);
}

hg_return_t Fam_Memory_Mercury_RPC::fam_memory_server_aggregation(hg_handle_t handle) {
                cout << "Entering fam_memory_server_aggregation _ Server Side " << endl;
                agg_flush_rpc_in_t request;
                hg_return_t ret;
                hg_bulk_t bulk_buffer;
                const struct hg_info *hgi;
                // Fam_Memory_Service_Direct *m1 = get_memory_service();

                ret = HG_Get_input(handle, &request);
                assert(ret == HG_SUCCESS);
                my_rpc_out_t response;

                // allocate fam data item for buffer and offset
                std::cout << "Bulk buffer in server : " << request.bulk_buffer
                          << std::endl;
                hgi = HG_Get_info(handle);
                hg_size_t size =
                    (hg_size_t)request.elementsize * request.nelements;

                // allocate fam data item for buffer and offset
                Fam_Region_Item_Info buffer_info;
                buffer_info = Fam_Memory_Mercury_RPC::memoryService->allocate(
                    ATOMIC_REGION_ID, size);
                // create local target buffer for bulk transfer
                void *buffer =
                    Fam_Memory_Mercury_RPC::memoryService->get_local_pointer(
                        ATOMIC_REGION_ID, buffer_info.offset);
                std::cout << "buffer is at :" << buffer << std::endl;

                ret = HG_Bulk_create(hgi->hg_class, 1, &buffer, &size,
                                     HG_BULK_WRITE_ONLY, &bulk_buffer);

                std::cout << "Transfering..... " << std::endl;
                // Do bulk transfer
                ret = HG_Bulk_transfer(hgi->context, my_rpc_handler_bulk_cb, 0,
                                       HG_BULK_PULL, hgi->addr,
                                       request.bulk_buffer, 0, bulk_buffer, 0,
                                       size, HG_OP_ID_IGNORE);
                sleep(5);

                hg_bulk_t bulk_offset;
                // Bulk transfer offset
                hg_size_t offset_size =
                    (hg_size_t)sizeof(uint64_t) * request.nelements;
                Fam_Region_Item_Info offset_info;
                offset_info = Fam_Memory_Mercury_RPC::memoryService->allocate(
                    ATOMIC_REGION_ID, offset_size);
                std::cout << "allocated :" << offset_info.base << " "
                          << offset_info.offset << std::endl;
                void *offset =
                    Fam_Memory_Mercury_RPC::memoryService->get_local_pointer(
                        ATOMIC_REGION_ID, offset_info.offset);

                ret = HG_Bulk_create(hgi->hg_class, 1, &offset, &offset_size,
                                     HG_BULK_WRITE_ONLY, &bulk_offset);
                // Do bulk transfer
                ret = HG_Bulk_transfer(hgi->context, my_rpc_handler_bulk_cb, 0,
                                       HG_BULK_PULL, hgi->addr,
                                       request.bulk_offset, 0, bulk_offset, 0,
                                       offset_size, HG_OP_ID_IGNORE);
                sleep(5);
#if 0
		std::cout<<"Printing data buffer:"<<std::endl;
		for(uint32_t i=0;i<size/sizeof(int);i++) {
          		std::cout<<*((uint64_t*)offset+i)<<" : "<<*((int*)buffer+i)<<"  ";
		}	
		std::cout<<std::endl;
#endif

                //bool result;
                try {
                    Fam_Memory_Mercury_RPC::memoryService
                        ->aggregate_indexed_add(
                            request.region_id, request.offset, request.opcode,
                            request.elementsize, request.nelements,
                            buffer_info.offset, offset_info.offset);
                } catch(Fam_Exception &e) {
                        response.errorcode = e.fam_error();
                        response.errormsg = strdup(e.fam_error_msg());
                }
                response.size = 123;
                response.errorcode = 0;
                cout << "Exiting from fam_memory_server_aggregation _ Server Side " << endl;
                ret = HG_Respond(handle, NULL, NULL, &response);
                assert(ret == HG_SUCCESS);
                (void) ret;
                HG_Destroy(handle);
                return ret;
}
}
