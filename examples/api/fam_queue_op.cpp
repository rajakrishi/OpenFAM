/*
 * api_fam_add.cpp
 * Copyright (c) 2020 Hewlett Packard Enterprise Development, LP. All rights
 * reserved. Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
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

#include <fam/fam.h>
#include <fam/fam_exception.h>
#include <string.h>
#include <fam/fam_extras.h>
#include <sched.h>
#include <iostream>
#include "get_my_time.h"

using namespace std;
using namespace openfam;

typedef struct {
  Fam_Descriptor *descriptor;
  uint64_t count;
  fam_extras *famext;
  uint64_t iterations;
} ValueInfo;
//#define MAX_VALUE 1000

pthread_barrier_t   barrier; // the barrier synchronization object

void *thr_fam_queue_op(void *arg) {
  ValueInfo *opInfo = (ValueInfo *)arg;
  Fam_Descriptor *descriptor = opInfo->descriptor;
  fam_extras *famext = opInfo->famext;
  uint64_t count = opInfo->count;
  uint64_t iterations = opInfo->iterations;
  //std::cout<<"Thread runing in "<<sched_getcpu()<<std::endl;
  uint64_t start,end;
  pthread_barrier_wait(&barrier);
  start = get_my_time();
  // fam_queue_op loop
  try {
    for (uint64_t i = 0; i < iterations; i++) {
      famext->fam_queue_operation(OP_PUT, descriptor, 1,
                                  1);
      famext->fam_queue_operation(OP_PUT, descriptor, 2,
                                  2);
      famext->fam_queue_operation(OP_PUT, descriptor, 3,
                                  3);
      famext->fam_queue_operation(OP_PUT, descriptor, 4,
                                 4);
      famext->fam_queue_operation(OP_PUT, descriptor, 5,
                                  5);
      famext->fam_queue_operation(OP_PUT, descriptor, 6,
                                  6);
      famext->fam_queue_operation(OP_PUT, descriptor, 7,
                                  7);
      famext->fam_queue_operation(OP_PUT, descriptor, 8,
                                  8);
      famext->fam_queue_operation(OP_PUT, descriptor, 9,
                                  9);
      famext->fam_queue_operation(OP_PUT, descriptor, 10,
                                  10);
      famext->fam_queue_operation(OP_PUT, descriptor, 11,
                                  11);
    }
  }
  catch (Fam_Exception &e) {
    printf("fam API failed: %d: %s %ld\n", e.fam_error(), e.fam_error_msg(),count);
    // ret = -1;
  }
  end = get_my_time();
  std::cout<<sched_getcpu()<<", Time, "<<(end-start)/(iterations*11)<<std::endl;
  pthread_exit(NULL);
  return NULL;
}

int main(int argc, char **argv) {
  int ret = 0;
  int iterations = 100;
  int NUM_THREADS = 1;
  if (argc == 3) {
    NUM_THREADS = atoi(argv[1]);
    iterations = atoi(argv[2]);
  }

  fam *myFam = new fam();

  Fam_Region_Descriptor *region = NULL;
  Fam_Descriptor *descriptor = NULL;
  Fam_Options *fm = (Fam_Options *)malloc(sizeof(Fam_Options));
  memset((void *)fm, 0, sizeof(Fam_Options));
  // assume that no specific options are needed by the implementation
  fm->runtime = strdup("NONE");
  try {
    myFam->fam_initialize("myApplication", fm);
    printf("FAM initialized\n");
  }
  catch (Fam_Exception &e) {
    printf("FAM Initialization failed: %s\n", e.fam_error_msg());
    myFam->fam_abort(-1); // abort the program
    // note that fam_abort currently returns after signaling
    // so we must terminate with the same value
    return -1;
  }

  int count = 100;
  // int MAX_VALUE = 1000;
  const int NUM_THREADS_MACRO = 256;
  pthread_t thr[NUM_THREADS_MACRO];
  // ... Initialization code here

  try {
    // create a 100 MB region with 0777 permissions and RAID5 redundancy
    region =
        myFam->fam_create_region("myRegion", (uint64_t)10000000, 0777, NULL);
    // create 50 element unnamed integer array in FAM with 0600
    // (read/write by owner) permissions in myRegion
    descriptor = myFam->fam_allocate("myItem", (uint64_t)(count * sizeof(int)),
                                     0600, region);
    // use the created region and data item...
    // ... continuation code here
    //
  }
  catch (Fam_Exception &e) {
    printf("Create region/Allocate Data item failed: %d: %s\n", e.fam_error(),
           e.fam_error_msg());
    region = myFam->fam_lookup_region("myRegion");
    descriptor = myFam->fam_lookup("myItem", "myRegion");
    // return -1;
  }

  try {
    // fam_queue_op here
    // int *local = (int *)malloc(10 * sizeof(int));
    fam_extras *famext = new fam_extras(myFam);
    famext->fam_queue_operation(OP_PUT, descriptor, 9, 9);
    //myFam->fam_reset_profile();
    ValueInfo *info;
    info = (ValueInfo *)malloc(sizeof(ValueInfo) * NUM_THREADS);
    pthread_barrier_init (&barrier, NULL, NUM_THREADS);

    // Call thread
    for (int i = 0; i < NUM_THREADS; ++i) {
      info[i].descriptor = descriptor;
      info[i].count = count;
      info[i].famext = famext;
      info[i].iterations = iterations;
      if ((ret = pthread_create(&thr[i], NULL, thr_fam_queue_op, &info[i]))) {
        fprintf(stderr, "error: pthread_create, rc: %d\n", ret);
        exit(1);
      }
    }
    for (int i = 0; i < NUM_THREADS; ++i) {
      pthread_join(thr[i], NULL);
    }
    famext->fam_aggregate_flush(descriptor);
    // ... subsequent code here
  }
  catch (Fam_Exception &e) {
    printf("fam API failed: %d: %s\n", e.fam_error(), e.fam_error_msg());
    ret = -1;
  }

  try {
    // we are finished. Destroy the region and everything in it
    myFam->fam_destroy_region(region);
    // printf("fam_destroy_region successfull\n");
  }
  catch (Fam_Exception &e) {
    printf("Destroy region failed: %d: %s\n", e.fam_error(), e.fam_error_msg());
    ret = -1;
  }

  // ... Finalization code follows
  try {
    myFam->fam_finalize("myApplication");
    printf("FAM finalized\n");
  }
  catch (Fam_Exception &e) {
    printf("FAM Finalization failed: %s\n", e.fam_error_msg());
    myFam->fam_abort(-1); // abort the program
    // note that fam_abort currently returns after signaling
    // so we must terminate with the same value
    return -1;
  }
  return (ret);
}
