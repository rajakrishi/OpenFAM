#ifndef FAM_MEMORY_MERCURY_RPC_H
#define FAM_MEMORY_MERCURY_RPC_H

#include "memory_service/fam_memory_service_direct.h"
#include "common/mercury_engine.h"

namespace openfam {

typedef struct {
    bool done;
    bool isFound;
    size_t maxKeyLen;
    Fam_Region_Item_Info itemInfo;
    pthread_cond_t doneCond;
    pthread_mutex_t doneMutex;
    //std::condition_variable done;
} Merc_RPC_State;

MERCURY_GEN_PROC(my_rpc_in_t, ((uint64_t)(key_region_id))((uint64_t)(key_dataitem_id))((hg_const_string_t)(key_region_name))((hg_const_string_t)(key_dataitem_name))((uint64_t)(region_id))((uint64_t)(offset))((uint32_t)(uid))((uint32_t)(gid))((uint64_t)(perm))((uint64_t)(size))((uint32_t)(user_policy))((uint64_t)(memsrv_id))((int32_t)(op)))
MERCURY_GEN_PROC(my_rpc_out_t, ((uint64_t)(region_id))((hg_const_string_t)(name))((uint64_t)(offset))((uint32_t)(uid))((uint32_t)(gid))((uint64_t)(perm))((uint64_t)(size))((uint64_t)(maxkeylen))((hg_bool_t)(isfound))((int32_t)(errorcode))((hg_const_string_t)(errormsg))((uint64_t)(memsrv_id)));

class Fam_Memory_Mercury_RPC {
    public:
        Fam_Memory_Mercury_RPC(const char *name,
                              const char *libfabricPort = NULL,
                              const char *libfabricProvider = NULL,
                              const char *fam_path = NULL,
                              bool isSharedMemory = false);
        Fam_Memory_Mercury_RPC() {}
        ~Fam_Memory_Mercury_RPC() {}
        hg_id_t register_with_mercury_fam_aggregation();
        Fam_Memory_Service_Direct *get_memory_service();
        static hg_return_t fam_memory_server_aggregation(hg_handle_t handle);

    private:
        static Fam_Memory_Service_Direct *memoryService;
};
}
#endif