/*
 * fam_ops_libfabric.cpp
 * Copyright (c) 2019-2021 Hewlett Packard Enterprise Development, LP. All
 * rights reserved. Redistribution and use in source and binary forms, with or
 * without modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * 3. Neither the name of the copyright holder nor the names of its contributors
 * may be used to endorse or promote products derived from this software without
 * specific prior written permission.
 *
 *    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *    INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * See https://spdx.org/licenses/BSD-3-Clause
 *
 */

#include <arpa/inet.h>
#include <iostream>
#include <sstream>
#include <stdlib.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <future>

#include "common/fam_internal.h"
#include "common/fam_libfabric.h"
#include "common/fam_ops.h"
#include "common/fam_ops_libfabric.h"
#include "fam/fam.h"
#include "fam/fam_exception.h"
#include "memory_service/fam_memory_mercury_rpc.h"

using namespace std;

namespace openfam {

Fam_Ops_Libfabric::~Fam_Ops_Libfabric() {

    delete contexts;
    delete defContexts;
    delete fiAddrs;
    delete memServerAddrs;
    delete fiMemsrvMap;
    delete fiMrs;
    free(service);
    free(provider);
    free(serverAddrName);
    hg_engine_finalize();
}

Fam_Ops_Libfabric::Fam_Ops_Libfabric(bool source, const char *libfabricProvider,
                                     Fam_Thread_Model famTM,
                                     Fam_Allocator_Client *famAlloc,
                                     Fam_Context_Model famCM) {
    std::ostringstream message;
    memoryServerName = NULL;
    service = NULL;
    provider = strdup(libfabricProvider);
    isSource = source;
    famThreadModel = famTM;
    famContextModel = famCM;
    famAllocator = famAlloc;

    fiAddrs = new std::vector<fi_addr_t>();
    memServerAddrs = new std::map<uint64_t, std::pair<void *, size_t>>();
    queue_map = new std::map<Fam_Descriptor*, boost::lockfree::queue<Fam_queue_request>*>();
    queue_op_map = new std::map<Fam_Descriptor *, queue_descriptor *>();
    queue_op_map_lock = new pthread_rwlock_t();
    pthread_rwlock_init(queue_op_map_lock, NULL);
    fiMemsrvMap = new std::map<uint64_t, fi_addr_t>();
    fiMrs = new std::map<uint64_t, Fam_Region_Map_t *>();
    contexts = new std::map<uint64_t, Fam_Context *>();
    defContexts = new std::map<uint64_t, Fam_Context *>();

    fi = NULL;
    fabric = NULL;
    eq = NULL;
    domain = NULL;
    av = NULL;
    serverAddrNameLen = 0;
    serverAddrName = NULL;

    numMemoryNodes = 0;
    if (!isSource && famAllocator == NULL) {
        message << "Fam Invalid Option Fam_Alloctor: NULL value specified"
                << famContextModel;
        THROW_ERR_MSG(Fam_InvalidOption_Exception, message.str().c_str());
    }
    //Rishi: Mercury Client code

    Fam_Memory_Mercury_RPC *mercuryRPC = new Fam_Memory_Mercury_RPC();
    hg_engine_init(HG_FALSE, provider);
    my_rpc_id = mercuryRPC->register_with_mercury_fam_aggregation();

    //const char *svr_addr_string=strdup("ofi+psm2://1a0b02:0");
    const char *svr_addr_string = getenv("MERC_SRV_ADDR");
    hg_engine_addr_lookup(svr_addr_string, &svr_addr);
    hg_engine_create_handle(svr_addr, my_rpc_id, &my_handle);
}

Fam_Ops_Libfabric::Fam_Ops_Libfabric(bool source, const char *libfabricProvider,
                                     Fam_Thread_Model famTM,
                                     Fam_Allocator_Client *famAlloc,
                                     Fam_Context_Model famCM,
                                     const char *memServerName,
                                     const char *libfabricPort) {
    std::ostringstream message;
    memoryServerName = strdup(memServerName);
    service = strdup(libfabricPort);
    provider = strdup(libfabricProvider);
    isSource = source;
    famThreadModel = famTM;
    famContextModel = famCM;
    famAllocator = famAlloc;

    fiAddrs = new std::vector<fi_addr_t>();
    memServerAddrs = new std::map<uint64_t, std::pair<void *, size_t>>();
    fiMemsrvMap = new std::map<uint64_t, fi_addr_t>();
    fiMrs = new std::map<uint64_t, Fam_Region_Map_t *>();
    contexts = new std::map<uint64_t, Fam_Context *>();
    defContexts = new std::map<uint64_t, Fam_Context *>();

    fi = NULL;
    fabric = NULL;
    eq = NULL;
    domain = NULL;
    av = NULL;
    serverAddrNameLen = 0;
    serverAddrName = NULL;

    numMemoryNodes = 0;
    if (!isSource && famAllocator == NULL) {
        message << "Fam Invalid Option Fam_Alloctor: NULL value specified"
                << famContextModel;
        THROW_ERR_MSG(Fam_InvalidOption_Exception, message.str().c_str());
    }
}

int Fam_Ops_Libfabric::initialize() {
    std::ostringstream message;
    int ret = 0;

    // Initialize the mutex lock
    (void)pthread_rwlock_init(&fiMrLock, NULL);

    // Initialize the mutex lock
    (void)pthread_rwlock_init(&fiMemsrvAddrLock, NULL);

    // Initialize the mutex lock
    if (famContextModel == FAM_CONTEXT_REGION)
        (void)pthread_mutex_init(&ctxLock, NULL);

    if ((ret = fabric_initialize(memoryServerName, service, isSource, provider,
                                 &fi, &fabric, &eq, &domain, famThreadModel)) <
        0) {
        return ret;
    }
    // Initialize address vector
    if (fi->ep_attr->type == FI_EP_RDM) {
        if ((ret = fabric_initialize_av(fi, domain, eq, &av)) < 0) {
            return ret;
        }
    }

    // Insert the memory server address into address vector
    // Only if it is not source
    if (!isSource) {
        numMemoryNodes = famAllocator->get_num_memory_servers();
        if (numMemoryNodes == 0) {
            message << "Libfabric initialize: memory server name not specified";
            THROW_ERR_MSG(Fam_Datapath_Exception, message.str().c_str());
        }
        size_t memServerInfoSize = 0;
        ret = famAllocator->get_memserverinfo_size(&memServerInfoSize);
        if (ret < 0) {
            message << "Fam allocator get_memserverinfo_size failed";
            THROW_ERRNO_MSG(Fam_Allocator_Exception, FAM_ERR_ALLOCATOR,
                            message.str().c_str());
        }

        if (memServerInfoSize) {
            void *memServerInfoBuffer = calloc(1, memServerInfoSize);
            ret = famAllocator->get_memserverinfo(memServerInfoBuffer);

            if (ret < 0) {
                message << "Fam Allocator get_memserverinfo failed";
                THROW_ERRNO_MSG(Fam_Allocator_Exception, FAM_ERR_ALLOCATOR,
                                message.str().c_str());
            }

            size_t bufPtr = 0;
            uint64_t nodeId;
            size_t addrSize;
            void *nodeAddr;
            uint64_t fiAddrsSize = fiAddrs->size();

            while (bufPtr < memServerInfoSize) {
                memcpy(&nodeId, ((char *)memServerInfoBuffer + bufPtr),
                       sizeof(uint64_t));
                bufPtr += sizeof(uint64_t);
                memcpy(&addrSize, ((char *)memServerInfoBuffer + bufPtr),
                       sizeof(size_t));
                bufPtr += sizeof(size_t);
                nodeAddr = calloc(1, addrSize);
                memcpy(nodeAddr, ((char *)memServerInfoBuffer + bufPtr),
                       addrSize);
                bufPtr += addrSize;
                // Save memory server address in memServerAddrs map
                memServerAddrs->insert(
                    {nodeId, std::make_pair(nodeAddr, addrSize)});

                // Initialize defaultCtx
                if (famContextModel == FAM_CONTEXT_DEFAULT) {
                    Fam_Context *defaultCtx =
                        new Fam_Context(fi, domain, famThreadModel);
                    defContexts->insert({nodeId, defaultCtx});
                    ret =
                        fabric_enable_bind_ep(fi, av, eq, defaultCtx->get_ep());
                    if (ret < 0) {
                        // TODO: Log error
                        return ret;
                    }
                }
                std::vector<fi_addr_t> tmpAddrV;
                ret = fabric_insert_av((char *)nodeAddr, av, &tmpAddrV);

                if (ret < 0) {
                    // TODO: Log error
                    return ret;
                }

                // Place the fi_addr_t at nodeId index of fiAddrs vector.
                if (nodeId >= fiAddrsSize) {
                    // Increase the size of fiAddrs vector to accomodate
                    // nodeId larger than the current size.
                    fiAddrs->resize(nodeId + 512, FI_ADDR_UNSPEC);
                    fiAddrsSize = fiAddrs->size();
                }
                fiAddrs->at(nodeId) = tmpAddrV[0];
            }
        }
    } else {
        // This is memory server. Populate the serverAddrName and
        // serverAddrNameLen from libfabric
        Fam_Context *tmpCtx = new Fam_Context(fi, domain, famThreadModel);
        ret = fabric_enable_bind_ep(fi, av, eq, tmpCtx->get_ep());
        if (ret < 0) {
            message << "Fam libfabric fabric_enable_bind_ep failed: "
                    << fabric_strerror(ret);
            THROW_ERR_MSG(Fam_Datapath_Exception, message.str().c_str());
        }

        serverAddrNameLen = 0;
        ret = fabric_getname_len(tmpCtx->get_ep(), &serverAddrNameLen);
        if (serverAddrNameLen <= 0) {
            message << "Fam libfabric fabric_getname_len failed: "
                    << fabric_strerror(ret);
            THROW_ERR_MSG(Fam_Datapath_Exception, message.str().c_str());
        }
        serverAddrName = calloc(1, serverAddrNameLen);
        ret = fabric_getname(tmpCtx->get_ep(), serverAddrName,
                             &serverAddrNameLen);
        if (ret < 0) {
            message << "Fam libfabric fabric_getname failed: "
                    << fabric_strerror(ret);
            THROW_ERR_MSG(Fam_Datapath_Exception, message.str().c_str());
        }

        // Save this context to defContexts on memoryserver
        defContexts->insert({0, tmpCtx});
    }
    fabric_iov_limit = fi->tx_attr->rma_iov_limit;

    return 0;
}

Fam_Context *Fam_Ops_Libfabric::get_context(Fam_Descriptor *descriptor) {
    std::ostringstream message;
    // Case - FAM_CONTEXT_DEFAULT
    if (famContextModel == FAM_CONTEXT_DEFAULT) {
        uint64_t nodeId = descriptor->get_memserver_id();
        return get_defaultCtx(nodeId);
    } else if (famContextModel == FAM_CONTEXT_REGION) {
        // Case - FAM_CONTEXT_REGION
        Fam_Context *ctx = (Fam_Context *)descriptor->get_context();
        if (ctx)
            return ctx;

        Fam_Global_Descriptor global = descriptor->get_global_descriptor();
        uint64_t regionId = global.regionId;
        int ret = 0;

        // ctx mutex lock
        (void)pthread_mutex_lock(&ctxLock);

        auto ctxObj = contexts->find(regionId);
        if (ctxObj == contexts->end()) {
            ctx = new Fam_Context(fi, domain, famThreadModel);
            contexts->insert({regionId, ctx});
            ret = fabric_enable_bind_ep(fi, av, eq, ctx->get_ep());
            if (ret < 0) {
                // ctx mutex unlock
                (void)pthread_mutex_unlock(&ctxLock);
                message << "Fam libfabric fabric_enable_bind_ep failed: "
                        << fabric_strerror(ret);
                THROW_ERR_MSG(Fam_Datapath_Exception, message.str().c_str());
            }
        } else {
            ctx = ctxObj->second;
        }
        descriptor->set_context(ctx);

        // ctx mutex unlock
        (void)pthread_mutex_unlock(&ctxLock);
        return ctx;
    } else {
        message << "Fam Invalid Option FAM_CONTEXT_MODEL: " << famContextModel;
        THROW_ERR_MSG(Fam_InvalidOption_Exception, message.str().c_str());
    }
}

void Fam_Ops_Libfabric::finalize() {
    fabric_finalize();
    if (fiMrs != NULL) {
        for (auto mr : *fiMrs) {
            Fam_Region_Map_t *fiRegionMap = mr.second;
            for (auto dmr : *(fiRegionMap->fiRegionMrs)) {
                fi_close(&(dmr.second->fid));
            }
            fiRegionMap->fiRegionMrs->clear();
            free(fiRegionMap);
        }
        fiMrs->clear();
    }

    if (contexts != NULL) {
        for (auto fam_ctx : *contexts) {
            delete fam_ctx.second;
        }
        contexts->clear();
    }

    if (defContexts != NULL) {
        for (auto fam_ctx : *defContexts) {
            delete fam_ctx.second;
        }
        defContexts->clear();
    }

    if (fi) {
        fi_freeinfo(fi);
        fi = NULL;
    }

    if (fabric) {
        fi_close(&fabric->fid);
        fabric = NULL;
    }

    if (eq) {
        fi_close(&eq->fid);
        eq = NULL;
    }

    if (domain) {
        fi_close(&domain->fid);
        domain = NULL;
    }

    if (av) {
        fi_close(&av->fid);
        av = NULL;
    }
    //hg_engine_finalize();
}

#if 0
void Fam_Ops_Libfabric::fam_queue_operation(FAM_QUEUE_OP op, Fam_Descriptor *descriptor,
                              int32_t value, uint64_t  elementIndex) {
     std::cout<<"At fam_ops libfabric"<<std::endl;
     Fam_queue_request req;
     boost::lockfree::queue<Fam_queue_request> *queue;
     req.op = op;
     req.value = value;
     req.elementIndex = elementIndex;
     // Find queue for this descriptor
     auto obj = queue_map->find(descriptor);
     if (obj == queue_map->end()) {
        std::cout<<"Creating new queue for desc :"<<descriptor;
        // Queue not present, create the queue
        queue = new boost::lockfree::queue<Fam_queue_request>(128);
        queue_map->insert({descriptor,queue});
     }
     else 
        queue = obj->second; 
     queue->push(req);
     std::cout<<"\n"<<req.op<<", Queued for "<<descriptor<<" "<<req.value<<" at "<<req.elementIndex<<std::endl;
     //queue_map.insert(descriptor,req);
     // Create request structure
     // Find the descriptor in map - if not found add into map
     // Add request structure to queue
     return;
}

void Fam_Ops_Libfabric::fam_aggregate_flush(Fam_Descriptor *descriptor) {
     std::cout<<"At fam_ops,flushing"<<std::endl;
     Fam_queue_request req;
     req.op = OP_PUT;
     req.value = 0;
     req.elementIndex = 0;
     boost::lockfree::queue<Fam_queue_request> *queue;
     auto obj = queue_map->find(descriptor);
     if (obj == queue_map->end()) {
          return;
     } else {
         queue = obj->second;
         std::cout<<"Flushing, Queue found :"<<queue<<std::endl;
     }
     std::map<uint64_t,int32_t> *reduction_map = new std::map<uint64_t,int32_t>;
     while(!queue->empty()) {         
         queue->pop(req);
         // Start client reduction
         // 1. Create an std::map, with offset as key 
         // 2. See if this offset is present in std::map
         //    of yes add req.value to value
         // 3. If not found insert 
         auto obj = reduction_map->find(req.elementIndex);
         if (obj == reduction_map->end()) {
             reduction_map->insert({req.elementIndex,req.value});
         }
         else {
              // req.elementIndex is found in map, modify the value
              obj->second = obj->second + req.value;
         }         
         std::cout<<"\n"<<req.op<<", Popped for "<<descriptor<<" "<<req.value<<" at "<<req.elementIndex<<std::endl;
     }
     // print values in std::map
     for (auto it = reduction_map->cbegin(); it != reduction_map->cend(); ++it) {
          std::cout << "{" << it->first << ": " << it->second << "}"<<std::endl;
     }
     return;
}
#endif
hg_return_t aggregate_cb(const struct hg_cb_info *info) {
    hg_return_t ret;
    ostringstream message;
    Merc_RPC_State *rpcState = (Merc_RPC_State *)info->arg;
    my_rpc_out_t resp;

    // cout << "In callback: " << info->ret<< endl;
    assert(info->ret == HG_SUCCESS);

    ret = HG_Get_output(info->info.forward.handle, &resp);
    assert(ret == 0);
    (void) ret;

    if(!resp.errorcode) {
        // cout << "resp.size= " << resp.size << endl;
        pthread_mutex_lock(&rpcState->doneMutex);
        rpcState->done = true;
        pthread_cond_signal(&rpcState->doneCond);
        pthread_mutex_unlock(&rpcState->doneMutex);
    } else {
        //rpcState->isFound = false;
        //rpcState->done = true;
        delete rpcState;
        message << resp.errormsg;
        THROW_ERR_MSG(Fam_Datapath_Exception, "aggregate not found");
    }
    HG_Free_output(info->info.forward.handle, &resp);
    //HG_Destroy(info->info.forward.handle);
    return HG_SUCCESS;
}
/*
 * 1. Check queue_op_map for queue_descriptor
 *  1a. If not found - create the queue_descriptor and add it into
 * queue_op_map
 * 2. Get memory server id from the request.
 * 3. Find the request buffer address for the given memory server from
 * queue_descriptor
 * 4. Get the next element from from request queue - Use atomic add
 * 5. Store data and element index.
 */

void Fam_Ops_Libfabric::fam_queue_operation(FAM_QUEUE_OP op,
                                            Fam_Descriptor *descriptor,
                                            int32_t value,
                                            uint64_t elementIndex) {

  queue_descriptor *qd;
  request_buffer *rbuf;

  qd = (queue_descriptor*)descriptor->get_queue_descriptor();
  if (qd == NULL) {
    // 
    // TODO: You need mutex here. As of now we have to make 
    // sure concurrent access will not happen
    //
    // Queue not present, create the queue
    qd = new queue_descriptor();
    qd->op = op;
    qd->max_elements = 131072*64;
    qd->elementsize = sizeof(int32_t);

    // TODO: Initialize rq buffer
    rbuf = new request_buffer();
    rbuf->buffer = new char[qd->max_elements * sizeof(int32_t)];
    rbuf->elementIndex = new uint64_t[qd->max_elements];
    rbuf->nElements = 0;
    qd->rq[0] = rbuf;
    descriptor->set_queue_descriptor(qd);
    // TODO: Give up lock
  } 

  // TODO: get memory server id from request
  // Now assuming only one memory server
  // TODO: Check if rq[id] is NULL, then initialize
  rbuf = qd->rq[0];
  // TODO: Use atomic adds
  //uint64_t buffer_index = rbuf->nElements.fetch_add(1);
  uint64_t buffer_index = rbuf->nElements.fetch_add(1);
  *((int *)rbuf->buffer + buffer_index) = value;
  *(rbuf->elementIndex + buffer_index) = elementIndex;
}

void Fam_Ops_Libfabric::fam_queue_operation(FAM_QUEUE_OP op, void *local,
                                            Fam_Descriptor *descriptor,
                                            uint64_t nElements,
                                            uint64_t *elementIndex,
                                            uint64_t elementSize) {
    queue_descriptor *qd;
    request_buffer *rbuf;

    qd = (queue_descriptor *)descriptor->get_queue_descriptor();
    if (qd == NULL) {
        //
        // TODO: You need mutex here. As of now we have to make
        // sure concurrent access will not happen
        //
        // Queue not present, create the queue
        qd = new queue_descriptor();
        qd->op = op;
        qd->max_elements = 131072 * 1024;
        qd->elementsize = sizeof(int32_t);

        // TODO: Initialize rq buffer
        rbuf = new request_buffer();
        rbuf->buffer = new char[qd->max_elements * sizeof(int32_t)];
        rbuf->elementIndex = new uint64_t[qd->max_elements];
        rbuf->nElements = 0;
        qd->rq[0] = rbuf;
        descriptor->set_queue_descriptor(qd);
        // TODO: Give up lock
    }

    // TODO: get memory server id from request
    // Now assuming only one memory server
    // TODO: Check if rq[id] is NULL, then initialize
    rbuf = qd->rq[0];
    if (rbuf->nElements + nElements >= qd->max_elements) {
        // std::cout << "Queue full" << std::endl;
        THROW_ERRNO_MSG(Fam_Datapath_Exception, get_fam_error(1), "Queue Full");
    }
    // TODO: Use atomic adds
    // uint64_t buffer_index = rbuf->nElements.fetch_add(1);
    char *value = (char *)local;
    for (uint64_t i = 0; i < nElements; i++) {
        uint64_t buffer_index = rbuf->nElements.fetch_add(1);
        *((int *)rbuf->buffer + buffer_index) = *(value + i * elementSize);
        *(rbuf->elementIndex + buffer_index) = *(elementIndex + i);
        // std::cout<<i<<" "<<*(value + i*elementSize)<<" "<<*(elementIndex +
        // i);
    }
}

void Fam_Ops_Libfabric::fam_aggregate_flush(Fam_Descriptor *descriptor) {
    // std::cout << __FILE__ << " " << __LINE__ << std::endl;
    if (descriptor==NULL){
	    return;
    }
    queue_descriptor *qd;
    request_buffer *rbuf;
    qd = (queue_descriptor *)descriptor->get_queue_descriptor();
    if (qd == NULL) {
        return;
    } else {
        // std::cout << "Flushing, Queue found :" << qd << " " << qd->op << " "
        //          << qd->max_elements << " " << qd->elementsize << std::endl;
    }
    rbuf = qd->rq[0];
    // std::cout << "Found buffer: " << rbuf->nElements << std::endl;

    // cout << "My RPC ID: " << my_rpc_id << endl;
    // TODO: Add Bulk transfer handles in request
    // Create bulk transfer handle 1 for buffer

    // Create bulk transfer handle 2
    //
    //ostringstream message;
    agg_flush_rpc_in_t req;
    req.region_id = descriptor->get_global_descriptor().regionId;
    req.offset = descriptor->get_global_descriptor().offset;
    req.opcode = 1001;
    req.elementsize = qd->elementsize;
    req.nelements = rbuf->nElements;
    (void)req;
    Merc_RPC_State *rpcState = new Merc_RPC_State();
    rpcState->done = false;
    rpcState->doneCond = PTHREAD_COND_INITIALIZER;
    rpcState->doneMutex = PTHREAD_MUTEX_INITIALIZER;

    // cout << " 2. My RPC ID: " << my_rpc_id << endl;
    //hg_handle_t my_handle;

    /* register buffer for rdma/bulk access by server */
    const struct hg_info *hgi = HG_Get_info(my_handle);
    hg_size_t size = (hg_size_t)(int)qd->elementsize * (int)rbuf->nElements;
    int ret = HG_Bulk_create(hgi->hg_class, 1, &rbuf->buffer, &size,
                             HG_BULK_READ_ONLY, &req.bulk_buffer);

    /* register offset for rdma/bulk access by server */
    hg_size_t offset_size =
        (hg_size_t)(int)sizeof(uint64_t) * (int)rbuf->nElements;
    ret = HG_Bulk_create(hgi->hg_class, 1, (void **)&rbuf->elementIndex,
                         &offset_size, HG_BULK_READ_ONLY, &req.bulk_offset);
    if (ret !=0 ) {
        std::cout<<"Return from bulk create : "<<HG_Error_to_string((hg_return_t)ret)<<" "<<offset_size<<" "<<size<<std::endl;
	if (hgi->hg_class == NULL)
		std::cout<<"NULL hg_class"<<std::endl;	
    }

    assert(ret == 0);
    // cout << " 3. My RPC ID: " << my_rpc_id << " " << req.bulk_buffer << endl;
    ret = HG_Forward(my_handle, aggregate_cb, rpcState, &req);
    // cout << "Return from HG_Forward : " << ret << std::endl;
    //assert(ret == 0);
    (void)ret;
    // cout << " 4. My RPC ID: " << my_rpc_id << endl;
    pthread_mutex_lock(&rpcState->doneMutex);
    while (!rpcState->done)
        pthread_cond_wait(&rpcState->doneCond, &rpcState->doneMutex);
    pthread_mutex_unlock(&rpcState->doneMutex);
    // cout << "Call done from client... Exiting " << endl;
    rbuf->nElements = 0;

    delete rpcState;
    ret = HG_Bulk_free(req.bulk_buffer);
    ret = HG_Bulk_free(req.bulk_offset);
    assert(ret == 0);
    // TODO: Send data to server
#if 0
     for(uint32_t i=0;i<rbuf->nElements;i++) {
        std::cout<<"Value : "<<*((int*)rbuf->buffer+i)<<" "<<*(rbuf->elementIndex+i)<<endl;
     }
#endif
}

#if 0
void Fam_Ops_Libfabric::fam_aggregate_flush(Fam_Descriptor *descriptor) {
  std::cout << __FILE__ << " " << __LINE__ << std::endl;
  queue_descriptor *qd;
  request_buffer *rbuf;
  qd = (queue_descriptor*)descriptor->get_queue_descriptor();
  if (qd == NULL) {
    return;
  } else {
    std::cout << "Flushing, Queue found :" << qd << " " << qd->op << " "
              << qd->max_elements << " " << qd->elementsize << std::endl;
  }
  rbuf = qd->rq[0];
  std::cout << "Found buffer: " << rbuf->nElements << std::endl;

    cout << "My RPC ID: " << my_rpc_id << endl;
    ostringstream message;
    Merc_RPC_State *rpcState = new Merc_RPC_State();
    my_rpc_in_t req;
    rpcState->done = false;
    rpcState->doneCond = PTHREAD_COND_INITIALIZER;
    rpcState->doneMutex = PTHREAD_MUTEX_INITIALIZER;

    cout << " 2. My RPC ID: " << my_rpc_id << endl;
    hg_handle_t my_handle;
    hg_engine_create_handle(svr_addr, my_rpc_id, &my_handle);
    cout << " 3. My RPC ID: " << my_rpc_id << endl;
    int ret = HG_Forward(my_handle, aggregate_cb, rpcState, &req);
    assert(ret == 0);
    (void) ret;

    cout << " 4. My RPC ID: " << my_rpc_id << endl;
    pthread_mutex_lock(&rpcState->doneMutex);
    while(!rpcState->done)
        pthread_cond_wait(&rpcState->doneCond, &rpcState->doneMutex);
    pthread_mutex_unlock(&rpcState->doneMutex);
    cout << "Call done from client... Exiting " << endl;

    delete rpcState;
  //TODO: Send data to server
#if 0
     for(uint32_t i=0;i<rbuf->nElements;i++) {
        std::cout<<"Value : "<<*((int*)rbuf->buffer+i)<<" "<<*(rbuf->elementIndex+i)<<endl;
     }
#endif
}
#endif
#if 0
hg_return_t aggregate_cb(const struct hg_cb_info *info) {
    hg_return_t ret;
    ostringstream message;
    Merc_RPC_State *rpcState = (Merc_RPC_State *)info->arg;
    my_rpc_out_t resp;


    assert(info->ret == HG_SUCCESS);
    cout << "In callback: " << endl;

    ret = HG_Get_output(info->info.forward.handle, &resp);
    assert(ret == 0);
    (void) ret;

    if(!resp.errorcode) {
        cout << "resp.size= " << resp.size << endl;
        pthread_mutex_lock(&rpcState->doneMutex);
        rpcState->done = true;
        pthread_cond_signal(&rpcState->doneCond);
        pthread_mutex_unlock(&rpcState->doneMutex);
    } else {
        //rpcState->isFound = false;
        //rpcState->done = true;
        delete rpcState;
        message << resp.errormsg;
        THROW_ERR_MSG(Fam_Datapath_Exception, "aggregate not found");
    }
    HG_Free_output(info->info.forward.handle, &resp);
    HG_Destroy(info->info.forward.handle);
    return HG_SUCCESS;
}
#endif
void Fam_Ops_Libfabric::fam_aggregate_poc(Fam_Descriptor *descriptor) {
    cout << "My RPC ID: " << my_rpc_id << endl;
    ostringstream message;
    Merc_RPC_State *rpcState = new Merc_RPC_State();
    my_rpc_in_t req;
    rpcState->done = false;
    rpcState->doneCond = PTHREAD_COND_INITIALIZER;
    rpcState->doneMutex = PTHREAD_MUTEX_INITIALIZER;

    hg_handle_t my_handle;
    hg_engine_create_handle(svr_addr, my_rpc_id, &my_handle);
    int ret = HG_Forward(my_handle, aggregate_cb, rpcState, &req);
    assert(ret == 0);
    (void) ret;

    pthread_mutex_lock(&rpcState->doneMutex);
    while(!rpcState->done)
        pthread_cond_wait(&rpcState->doneCond, &rpcState->doneMutex);
    pthread_mutex_unlock(&rpcState->doneMutex);
    cout << "Call done from client... Exiting " << endl;

    delete rpcState;
}
int Fam_Ops_Libfabric::put_blocking(void *local, Fam_Descriptor *descriptor,
                                    uint64_t offset, uint64_t nbytes) {
    std::ostringstream message;
    // Write data into memory region with this key
    uint64_t key;
    key = descriptor->get_key();
    offset += (uint64_t)descriptor->get_base_address();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int ret = fabric_write(key, local, nbytes, offset, (*fiAddr)[nodeId],
                           get_context(descriptor));
    return ret;
}

int Fam_Ops_Libfabric::get_blocking(void *local, Fam_Descriptor *descriptor,
                                    uint64_t offset, uint64_t nbytes) {
    std::ostringstream message;
    // Write data into memory region with this key
    uint64_t key;
    key = descriptor->get_key();
    offset += (uint64_t)descriptor->get_base_address();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int ret = fabric_read(key, local, nbytes, offset, (*fiAddr)[nodeId],
                          get_context(descriptor));

    return ret;
}

int Fam_Ops_Libfabric::gather_blocking(void *local, Fam_Descriptor *descriptor,
                                       uint64_t nElements,
                                       uint64_t firstElement, uint64_t stride,
                                       uint64_t elementSize) {

    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int ret = fabric_gather_stride_blocking(
        key, local, elementSize, firstElement, nElements, stride,
        (*fiAddr)[nodeId], get_context(descriptor), fabric_iov_limit,
        (uint64_t)descriptor->get_base_address());
    return ret;
}

int Fam_Ops_Libfabric::gather_blocking(void *local, Fam_Descriptor *descriptor,
                                       uint64_t nElements,
                                       uint64_t *elementIndex,
                                       uint64_t elementSize) {
    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int ret = fabric_gather_index_blocking(
        key, local, elementSize, elementIndex, nElements, (*fiAddr)[nodeId],
        get_context(descriptor), fabric_iov_limit,
        (uint64_t)descriptor->get_base_address());
    return ret;
}

int Fam_Ops_Libfabric::scatter_blocking(void *local, Fam_Descriptor *descriptor,
                                        uint64_t nElements,
                                        uint64_t firstElement, uint64_t stride,
                                        uint64_t elementSize) {

    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int ret = fabric_scatter_stride_blocking(
        key, local, elementSize, firstElement, nElements, stride,
        (*fiAddr)[nodeId], get_context(descriptor), fabric_iov_limit,
        (uint64_t)descriptor->get_base_address());
    return ret;
}

int Fam_Ops_Libfabric::scatter_blocking(void *local, Fam_Descriptor *descriptor,
                                        uint64_t nElements,
                                        uint64_t *elementIndex,
                                        uint64_t elementSize) {
    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int ret = fabric_scatter_index_blocking(
        key, local, elementSize, elementIndex, nElements, (*fiAddr)[nodeId],
        get_context(descriptor), fabric_iov_limit,
        (uint64_t)descriptor->get_base_address());
    return ret;
}

void Fam_Ops_Libfabric::put_nonblocking(void *local, Fam_Descriptor *descriptor,
                                        uint64_t offset, uint64_t nbytes) {

    uint64_t key;

    key = descriptor->get_key();
    offset += (uint64_t)descriptor->get_base_address();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_write_nonblocking(key, local, nbytes, offset, (*fiAddr)[nodeId],
                             get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::get_nonblocking(void *local, Fam_Descriptor *descriptor,
                                        uint64_t offset, uint64_t nbytes) {
    uint64_t key;

    key = descriptor->get_key();
    offset += (uint64_t)descriptor->get_base_address();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_read_nonblocking(key, local, nbytes, offset, (*fiAddr)[nodeId],
                            get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::gather_nonblocking(
    void *local, Fam_Descriptor *descriptor, uint64_t nElements,
    uint64_t firstElement, uint64_t stride, uint64_t elementSize) {

    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_gather_stride_nonblocking(key, local, elementSize, firstElement,
                                     nElements, stride, (*fiAddr)[nodeId],
                                     get_context(descriptor), fabric_iov_limit,
                                     (uint64_t)descriptor->get_base_address());
    return;
}

void Fam_Ops_Libfabric::gather_nonblocking(void *local,
                                           Fam_Descriptor *descriptor,
                                           uint64_t nElements,
                                           uint64_t *elementIndex,
                                           uint64_t elementSize) {
    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_gather_index_nonblocking(key, local, elementSize, elementIndex,
                                    nElements, (*fiAddr)[nodeId],
                                    get_context(descriptor), fabric_iov_limit,
                                    (uint64_t)descriptor->get_base_address());
    return;
}

void Fam_Ops_Libfabric::scatter_nonblocking(
    void *local, Fam_Descriptor *descriptor, uint64_t nElements,
    uint64_t firstElement, uint64_t stride, uint64_t elementSize) {

    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_scatter_stride_nonblocking(key, local, elementSize, firstElement,
                                      nElements, stride, (*fiAddr)[nodeId],
                                      get_context(descriptor), fabric_iov_limit,
                                      (uint64_t)descriptor->get_base_address());
    return;
}

void Fam_Ops_Libfabric::scatter_nonblocking(void *local,
                                            Fam_Descriptor *descriptor,
                                            uint64_t nElements,
                                            uint64_t *elementIndex,
                                            uint64_t elementSize) {
    uint64_t key;

    key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_scatter_index_nonblocking(key, local, elementSize, elementIndex,
                                     nElements, (*fiAddr)[nodeId],
                                     get_context(descriptor), fabric_iov_limit,
                                     (uint64_t)descriptor->get_base_address());
    return;
}

// Note : In case of copy operation across memoryserver this API is blocking
// and no need to wait on copy.
void *Fam_Ops_Libfabric::copy(Fam_Descriptor *src, uint64_t srcOffset,
                              Fam_Descriptor *dest, uint64_t destOffset,
                              uint64_t nbytes) {
    // Perform actual copy operation at the destination memory server
    // Send additional information to destination:
    // source addr len, source addr

    std::pair<void *, size_t> srcMemSrv;
    auto obj = memServerAddrs->find(src->get_memserver_id());
    if (obj == memServerAddrs->end())
        THROW_ERR_MSG(Fam_Datapath_Exception, "memserver not found");
    else
        srcMemSrv = obj->second;

    return famAllocator->copy(src, srcOffset, (const char *)srcMemSrv.first,
                              (uint32_t)srcMemSrv.second, dest, destOffset,
                              nbytes);
}

void Fam_Ops_Libfabric::wait_for_copy(void *waitObj) {
    return famAllocator->wait_for_copy(waitObj);
}

void *Fam_Ops_Libfabric::backup(Fam_Descriptor *descriptor, char *BackupName) {

    return famAllocator->backup(descriptor, BackupName);
}

void *Fam_Ops_Libfabric::restore(char *BackupName, Fam_Descriptor *dest,
                                 uint64_t size) {

    return famAllocator->restore(dest, BackupName, size);
}

void Fam_Ops_Libfabric::wait_for_backup(void *waitObj) {
    return famAllocator->wait_for_backup(waitObj);
}
void Fam_Ops_Libfabric::wait_for_restore(void *waitObj) {
    return famAllocator->wait_for_restore(waitObj);
}

void Fam_Ops_Libfabric::fence(Fam_Region_Descriptor *descriptor) {
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();

    uint64_t nodeId = 0;
    if (famContextModel == FAM_CONTEXT_DEFAULT) {
        for (auto fam_ctx : *defContexts) {
            nodeId = fam_ctx.first;
            fabric_fence((*fiAddr)[nodeId], fam_ctx.second);
        }
    } else if (famContextModel == FAM_CONTEXT_REGION) {
        // ctx mutex lock
        (void)pthread_mutex_lock(&ctxLock);

        try {
            if (descriptor) {
                nodeId = descriptor->get_memserver_id();
                Fam_Context *ctx = (Fam_Context *)descriptor->get_context();
                if (ctx) {
                    fabric_fence((*fiAddr)[nodeId], ctx);
                } else {
                    Fam_Global_Descriptor global =
                        descriptor->get_global_descriptor();
                    uint64_t regionId = global.regionId;
                    auto ctxObj = contexts->find(regionId);
                    if (ctxObj != contexts->end()) {
                        descriptor->set_context(ctxObj->second);
                        fabric_fence((*fiAddr)[nodeId], ctxObj->second);
                    }
                }
            } else {
                for (auto fam_ctx : *contexts) {
                    fabric_fence(
                        (*fiAddr)[(fam_ctx.first) >> MEMSERVERID_SHIFT],
                        fam_ctx.second);
                }
            }
        } catch (...) {
            // ctx mutex unlock
            (void)pthread_mutex_unlock(&ctxLock);
            throw;
        }
        // ctx mutex unlock
        (void)pthread_mutex_unlock(&ctxLock);
    }
}

void Fam_Ops_Libfabric::check_progress(Fam_Region_Descriptor *descriptor) {
    if (famContextModel == FAM_CONTEXT_DEFAULT) {

        for (auto context : *defContexts) {
            Fam_Context *famCtx = context.second;
            uint64_t success = fi_cntr_read(famCtx->get_txCntr());
            success += fi_cntr_read(famCtx->get_rxCntr());
        }
    }
    return;
}

void Fam_Ops_Libfabric::quiet_context(Fam_Context *context = NULL) {
    if (famContextModel == FAM_CONTEXT_DEFAULT) {
        std::list<std::shared_future<void>> resultList;
        int err = 0;
        std::string errmsg;
        int exception_caught = 0;

        for (auto context : *defContexts) {
            std::future<void> result =
                (std::async(std::launch::async, fabric_quiet, context.second));
            resultList.push_back(result.share());
        }
        for (auto result : resultList) {

            try {
                result.get();
            } catch (Fam_Exception &e) {
                err = e.fam_error();
                errmsg = e.fam_error_msg();
                exception_caught = 1;
            }
        }

        if (exception_caught == 1) {
            THROW_ERRNO_MSG(Fam_Datapath_Exception, get_fam_error(err), errmsg);
        }

    } else if (famContextModel == FAM_CONTEXT_REGION) {
        fabric_quiet(context);
    }
    return;
}

void Fam_Ops_Libfabric::quiet(Fam_Region_Descriptor *descriptor) {
    if (famContextModel == FAM_CONTEXT_DEFAULT) {
        quiet_context();
        return;
    } else if (famContextModel == FAM_CONTEXT_REGION) {
        // ctx mutex lock
        (void)pthread_mutex_lock(&ctxLock);
        try {
            if (descriptor) {
                Fam_Context *ctx = (Fam_Context *)descriptor->get_context();
                if (ctx) {
                    quiet_context(ctx);
                } else {
                    Fam_Global_Descriptor global =
                        descriptor->get_global_descriptor();
                    uint64_t regionId = global.regionId;
                    auto ctxObj = contexts->find(regionId);
                    if (ctxObj != contexts->end()) {
                        descriptor->set_context(ctxObj->second);
                        quiet_context(ctxObj->second);
                    }
                }
            } else {
                for (auto fam_ctx : *contexts)
                    quiet_context(fam_ctx.second);
            }
        } catch (...) {
            // ctx mutex unlock
            (void)pthread_mutex_unlock(&ctxLock);
            throw;
        }
        // ctx mutex unlock
        (void)pthread_mutex_unlock(&ctxLock);
    }
}

uint64_t Fam_Ops_Libfabric::progress_context() {
    uint64_t pending = 0;
    for (auto fam_ctx : *defContexts) {
        pending += fabric_progress(fam_ctx.second);
    }
    return pending;
}

uint64_t Fam_Ops_Libfabric::progress() { return progress_context(); }
void Fam_Ops_Libfabric::atomic_set(Fam_Descriptor *descriptor, uint64_t offset,
                                   int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_ATOMIC_WRITE, FI_INT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_set(Fam_Descriptor *descriptor, uint64_t offset,
                                   int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_ATOMIC_WRITE, FI_INT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_set(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_ATOMIC_WRITE, FI_UINT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_set(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_ATOMIC_WRITE, FI_UINT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_set(Fam_Descriptor *descriptor, uint64_t offset,
                                   float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_ATOMIC_WRITE, FI_FLOAT,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_set(Fam_Descriptor *descriptor, uint64_t offset,
                                   double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_ATOMIC_WRITE, FI_DOUBLE,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_add(Fam_Descriptor *descriptor, uint64_t offset,
                                   int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_SUM, FI_INT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_add(Fam_Descriptor *descriptor, uint64_t offset,
                                   int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_SUM, FI_INT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_add(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_SUM, FI_UINT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_add(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_SUM, FI_UINT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_add(Fam_Descriptor *descriptor, uint64_t offset,
                                   float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_SUM, FI_FLOAT,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_add(Fam_Descriptor *descriptor, uint64_t offset,
                                   double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_SUM, FI_DOUBLE,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_subtract(Fam_Descriptor *descriptor,
                                        uint64_t offset, int32_t value) {
    atomic_add(descriptor, offset, -value);
    return;
}

void Fam_Ops_Libfabric::atomic_subtract(Fam_Descriptor *descriptor,
                                        uint64_t offset, int64_t value) {
    atomic_add(descriptor, offset, -value);
    return;
}

void Fam_Ops_Libfabric::atomic_subtract(Fam_Descriptor *descriptor,
                                        uint64_t offset, uint32_t value) {
    atomic_add(descriptor, offset, -value);
    return;
}

void Fam_Ops_Libfabric::atomic_subtract(Fam_Descriptor *descriptor,
                                        uint64_t offset, uint64_t value) {
    atomic_add(descriptor, offset, -value);
    return;
}

void Fam_Ops_Libfabric::atomic_subtract(Fam_Descriptor *descriptor,
                                        uint64_t offset, float value) {
    atomic_add(descriptor, offset, -value);
    return;
}

void Fam_Ops_Libfabric::atomic_subtract(Fam_Descriptor *descriptor,
                                        uint64_t offset, double value) {
    atomic_add(descriptor, offset, -value);
    return;
}

void Fam_Ops_Libfabric::atomic_min(Fam_Descriptor *descriptor, uint64_t offset,
                                   int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MIN, FI_INT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_min(Fam_Descriptor *descriptor, uint64_t offset,
                                   int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MIN, FI_INT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_min(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MIN, FI_UINT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_min(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MIN, FI_UINT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_min(Fam_Descriptor *descriptor, uint64_t offset,
                                   float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MIN, FI_FLOAT,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_min(Fam_Descriptor *descriptor, uint64_t offset,
                                   double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MIN, FI_DOUBLE,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_max(Fam_Descriptor *descriptor, uint64_t offset,
                                   int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MAX, FI_INT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_max(Fam_Descriptor *descriptor, uint64_t offset,
                                   int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MAX, FI_INT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_max(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MAX, FI_UINT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_max(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MAX, FI_UINT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_max(Fam_Descriptor *descriptor, uint64_t offset,
                                   float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MAX, FI_FLOAT,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_max(Fam_Descriptor *descriptor, uint64_t offset,
                                   double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_MAX, FI_DOUBLE,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_and(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_BAND, FI_UINT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_and(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_BAND, FI_UINT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_or(Fam_Descriptor *descriptor, uint64_t offset,
                                  uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_BOR, FI_UINT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_or(Fam_Descriptor *descriptor, uint64_t offset,
                                  uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_BOR, FI_UINT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_xor(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_BXOR, FI_UINT32,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

void Fam_Ops_Libfabric::atomic_xor(Fam_Descriptor *descriptor, uint64_t offset,
                                   uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    fabric_atomic(key, (void *)&value, offset, FI_BXOR, FI_UINT64,
                  (*fiAddr)[nodeId], get_context(descriptor));
    return;
}

int32_t Fam_Ops_Libfabric::swap(Fam_Descriptor *descriptor, uint64_t offset,
                                int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset,
                        FI_ATOMIC_WRITE, FI_INT32, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return old;
}

int64_t Fam_Ops_Libfabric::swap(Fam_Descriptor *descriptor, uint64_t offset,
                                int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset,
                        FI_ATOMIC_WRITE, FI_INT64, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::swap(Fam_Descriptor *descriptor, uint64_t offset,
                                 uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset,
                        FI_ATOMIC_WRITE, FI_UINT32, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::swap(Fam_Descriptor *descriptor, uint64_t offset,
                                 uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset,
                        FI_ATOMIC_WRITE, FI_UINT64, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return old;
}

float Fam_Ops_Libfabric::swap(Fam_Descriptor *descriptor, uint64_t offset,
                              float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    float old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset,
                        FI_ATOMIC_WRITE, FI_FLOAT, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return old;
}

double Fam_Ops_Libfabric::swap(Fam_Descriptor *descriptor, uint64_t offset,
                               double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    double old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset,
                        FI_ATOMIC_WRITE, FI_DOUBLE, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return old;
}

int32_t Fam_Ops_Libfabric::compare_swap(Fam_Descriptor *descriptor,
                                        uint64_t offset, int32_t oldValue,
                                        int32_t newValue) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int32_t old;
    fabric_compare_atomic(key, (void *)&oldValue, (void *)&old,
                          (void *)&newValue, offset, FI_CSWAP, FI_INT32,
                          (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

int64_t Fam_Ops_Libfabric::compare_swap(Fam_Descriptor *descriptor,
                                        uint64_t offset, int64_t oldValue,
                                        int64_t newValue) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int64_t old;
    fabric_compare_atomic(key, (void *)&oldValue, (void *)&old,
                          (void *)&newValue, offset, FI_CSWAP, FI_INT64,
                          (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::compare_swap(Fam_Descriptor *descriptor,
                                         uint64_t offset, uint32_t oldValue,
                                         uint32_t newValue) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_compare_atomic(key, (void *)&oldValue, (void *)&old,
                          (void *)&newValue, offset, FI_CSWAP, FI_UINT32,
                          (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::compare_swap(Fam_Descriptor *descriptor,
                                         uint64_t offset, uint64_t oldValue,
                                         uint64_t newValue) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_compare_atomic(key, (void *)&oldValue, (void *)&old,
                          (void *)&newValue, offset, FI_CSWAP, FI_UINT64,
                          (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

int128_t Fam_Ops_Libfabric::compare_swap(Fam_Descriptor *descriptor,
                                         uint64_t offset, int128_t oldValue,
                                         int128_t newValue) {

    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int128_t local;

    famAllocator->acquire_CAS_lock(descriptor);
    try {
        fabric_read(key, &local, sizeof(int128_t), offset, (*fiAddr)[nodeId],
                    get_context(descriptor));
    } catch (...) {
        famAllocator->release_CAS_lock(descriptor);
        throw;
    }

    if (local == oldValue) {
        try {
            fabric_write(key, &newValue, sizeof(int128_t), offset,
                         (*fiAddr)[nodeId], get_context(descriptor));
        } catch (...) {
            famAllocator->release_CAS_lock(descriptor);
            throw;
        }
    }
    famAllocator->release_CAS_lock(descriptor);
    return local;
}

int32_t Fam_Ops_Libfabric::atomic_fetch_int32(Fam_Descriptor *descriptor,
                                              uint64_t offset) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int32_t result;
    fabric_fetch_atomic(key, (void *)&result, (void *)&result, offset,
                        FI_ATOMIC_READ, FI_INT32, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return result;
}

int64_t Fam_Ops_Libfabric::atomic_fetch_int64(Fam_Descriptor *descriptor,
                                              uint64_t offset) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int64_t result;
    fabric_fetch_atomic(key, (void *)&result, (void *)&result, offset,
                        FI_ATOMIC_READ, FI_INT64, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return result;
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_uint32(Fam_Descriptor *descriptor,
                                                uint64_t offset) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t result;
    fabric_fetch_atomic(key, (void *)&result, (void *)&result, offset,
                        FI_ATOMIC_READ, FI_UINT32, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return result;
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_uint64(Fam_Descriptor *descriptor,
                                                uint64_t offset) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t result;
    fabric_fetch_atomic(key, (void *)&result, (void *)&result, offset,
                        FI_ATOMIC_READ, FI_UINT64, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return result;
}

float Fam_Ops_Libfabric::atomic_fetch_float(Fam_Descriptor *descriptor,
                                            uint64_t offset) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    float result;
    fabric_fetch_atomic(key, (void *)&result, (void *)&result, offset,
                        FI_ATOMIC_READ, FI_FLOAT, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return result;
}

double Fam_Ops_Libfabric::atomic_fetch_double(Fam_Descriptor *descriptor,
                                              uint64_t offset) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    double result;
    fabric_fetch_atomic(key, (void *)&result, (void *)&result, offset,
                        FI_ATOMIC_READ, FI_DOUBLE, (*fiAddr)[nodeId],
                        get_context(descriptor));
    return result;
}

int32_t Fam_Ops_Libfabric::atomic_fetch_add(Fam_Descriptor *descriptor,
                                            uint64_t offset, int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_SUM,
                        FI_INT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

int64_t Fam_Ops_Libfabric::atomic_fetch_add(Fam_Descriptor *descriptor,
                                            uint64_t offset, int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_SUM,
                        FI_INT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_add(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_SUM,
                        FI_UINT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_add(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_SUM,
                        FI_UINT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

float Fam_Ops_Libfabric::atomic_fetch_add(Fam_Descriptor *descriptor,
                                          uint64_t offset, float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    float old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_SUM,
                        FI_FLOAT, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

double Fam_Ops_Libfabric::atomic_fetch_add(Fam_Descriptor *descriptor,
                                           uint64_t offset, double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    double old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_SUM,
                        FI_DOUBLE, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

int32_t Fam_Ops_Libfabric::atomic_fetch_subtract(Fam_Descriptor *descriptor,
                                                 uint64_t offset,
                                                 int32_t value) {
    return atomic_fetch_add(descriptor, offset, -value);
}

int64_t Fam_Ops_Libfabric::atomic_fetch_subtract(Fam_Descriptor *descriptor,
                                                 uint64_t offset,
                                                 int64_t value) {
    return atomic_fetch_add(descriptor, offset, -value);
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_subtract(Fam_Descriptor *descriptor,
                                                  uint64_t offset,
                                                  uint32_t value) {
    return atomic_fetch_add(descriptor, offset, -value);
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_subtract(Fam_Descriptor *descriptor,
                                                  uint64_t offset,
                                                  uint64_t value) {
    return atomic_fetch_add(descriptor, offset, -value);
}

float Fam_Ops_Libfabric::atomic_fetch_subtract(Fam_Descriptor *descriptor,
                                               uint64_t offset, float value) {
    return atomic_fetch_add(descriptor, offset, -value);
}

double Fam_Ops_Libfabric::atomic_fetch_subtract(Fam_Descriptor *descriptor,
                                                uint64_t offset, double value) {
    return atomic_fetch_add(descriptor, offset, -value);
}

int32_t Fam_Ops_Libfabric::atomic_fetch_min(Fam_Descriptor *descriptor,
                                            uint64_t offset, int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MIN,
                        FI_INT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

int64_t Fam_Ops_Libfabric::atomic_fetch_min(Fam_Descriptor *descriptor,
                                            uint64_t offset, int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MIN,
                        FI_INT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_min(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MIN,
                        FI_UINT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_min(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MIN,
                        FI_UINT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

float Fam_Ops_Libfabric::atomic_fetch_min(Fam_Descriptor *descriptor,
                                          uint64_t offset, float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    float old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MIN,
                        FI_FLOAT, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

double Fam_Ops_Libfabric::atomic_fetch_min(Fam_Descriptor *descriptor,
                                           uint64_t offset, double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    double old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MIN,
                        FI_DOUBLE, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

int32_t Fam_Ops_Libfabric::atomic_fetch_max(Fam_Descriptor *descriptor,
                                            uint64_t offset, int32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MAX,
                        FI_INT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

int64_t Fam_Ops_Libfabric::atomic_fetch_max(Fam_Descriptor *descriptor,
                                            uint64_t offset, int64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    int64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MAX,
                        FI_INT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_max(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MAX,
                        FI_UINT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_max(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MAX,
                        FI_UINT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

float Fam_Ops_Libfabric::atomic_fetch_max(Fam_Descriptor *descriptor,
                                          uint64_t offset, float value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    float old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MAX,
                        FI_FLOAT, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

double Fam_Ops_Libfabric::atomic_fetch_max(Fam_Descriptor *descriptor,
                                           uint64_t offset, double value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    double old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_MAX,
                        FI_DOUBLE, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_and(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_BAND,
                        FI_UINT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_and(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_BAND,
                        FI_UINT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_or(Fam_Descriptor *descriptor,
                                            uint64_t offset, uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_BOR,
                        FI_UINT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_or(Fam_Descriptor *descriptor,
                                            uint64_t offset, uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_BOR,
                        FI_UINT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint32_t Fam_Ops_Libfabric::atomic_fetch_xor(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint32_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint32_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_BXOR,
                        FI_UINT32, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

uint64_t Fam_Ops_Libfabric::atomic_fetch_xor(Fam_Descriptor *descriptor,
                                             uint64_t offset, uint64_t value) {
    std::ostringstream message;
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    uint64_t old;
    fabric_fetch_atomic(key, (void *)&value, (void *)&old, offset, FI_BXOR,
                        FI_UINT64, (*fiAddr)[nodeId], get_context(descriptor));
    return old;
}

void Fam_Ops_Libfabric::abort(int status) FAM_OPS_UNIMPLEMENTED(void__);

void Fam_Ops_Libfabric::atomic_set(Fam_Descriptor *descriptor, uint64_t offset,
                                   int128_t value) {
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    famAllocator->acquire_CAS_lock(descriptor);
    try {
        fabric_write(key, &value, sizeof(int128_t), offset, (*fiAddr)[nodeId],
                     get_context(descriptor));
    } catch (...) {
        famAllocator->release_CAS_lock(descriptor);
        throw;
    }
    famAllocator->release_CAS_lock(descriptor);
}

int128_t Fam_Ops_Libfabric::atomic_fetch_int128(Fam_Descriptor *descriptor,
                                                uint64_t offset) {
    uint64_t key = descriptor->get_key();
    uint64_t nodeId = descriptor->get_memserver_id();
    offset += (uint64_t)descriptor->get_base_address();

    int128_t local;
    std::vector<fi_addr_t> *fiAddr = get_fiAddrs();
    famAllocator->acquire_CAS_lock(descriptor);
    try {
        fabric_read(key, &local, sizeof(int128_t), offset, (*fiAddr)[nodeId],
                    get_context(descriptor));
    } catch (...) {
        famAllocator->release_CAS_lock(descriptor);
        throw;
    }
    famAllocator->release_CAS_lock(descriptor);
    return local;
}

} // namespace openfam
