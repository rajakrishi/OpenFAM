#include "memory_service/fam_memory_mercury_rpc.h"

namespace openfam {

Fam_Memory_Service_Direct *Fam_Memory_Mercury_RPC::memoryService;
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

    tmp = MERCURY_REGISTER(
        hg_class, "memory_server_aggregation", my_rpc_in_t, my_rpc_out_t, Fam_Memory_Mercury_RPC::fam_memory_server_aggregation);

    return (tmp);
}

hg_return_t Fam_Memory_Mercury_RPC::fam_memory_server_aggregation(hg_handle_t handle) {
                cout << "Entering fam_memory_server_aggregation _ Server Side " << endl;
                my_rpc_in_t request;
                hg_return_t ret;

                ret = HG_Get_input(handle, &request);
                assert(ret == HG_SUCCESS);
                my_rpc_out_t response;
                //bool result;
                try {
                    Fam_Memory_Mercury_RPC::memoryService->fam_aggregation_poc();
                } catch(Fam_Exception &e) {
                        response.errorcode = e.fam_error();
                        response.errormsg = strdup(e.fam_error_msg());
                }
                response.size = 123;
                response.errorcode = 0;
                cout << "Exiting from fam_memory_server_aggregation _ Server Side " << endl;
#if 0
                Fam_DataItem_Memory dataitem;
                try {
                        Fam_Memory_Mercury_RPC::memoryService->metadata_find_dataitem_and_check_permissions((metadata_region_item_op_t)request.op, request.key_dataitem_name, request.key_region_name, request.uid, request.gid, dataitem);
                } catch(Fam_Exception &e) {
                        //response.isfound = false;
                        response.errorcode = e.fam_error();
                        response.errormsg = strdup(e.fam_error_msg());
                }
                response.region_id = dataitem.regionId;
                response.name = dataitem.name;
                response.offset = dataitem.offset;
                response.size = dataitem.size;
                response.perm = dataitem.perm;
                response.uid = dataitem.uid;
                response.gid = dataitem.gid;
                response.maxkeylen = Fam_Memory_Mercury_RPC::memoryService->metadata_maxkeylen();
                response.memsrv_id = dataitem.memoryServerId;
                //response.isfound = result;
                response.errorcode = 0;
                response.errormsg = strdup("");

                //cout << "Original::data item name : " << dataitem.name << " Region Id : " << dataitem.regionId << " Offset : " << dataitem.offset << endl;
                //cout << "Response::data item name : " << response.name << " Region Id : " << response.region_id << " Offset : " << response.offset << endl;
#endif
                ret = HG_Respond(handle, NULL, NULL, &response);
                assert(ret == HG_SUCCESS);
                (void) ret;
                HG_Destroy(handle);
                return ret;
}
}
