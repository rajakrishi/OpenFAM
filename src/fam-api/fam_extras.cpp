/*
 * fam_extras.cpp
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
#include <iostream>
#include <sstream>

#include "fam/fam.h"
#include "fam/fam_extras.h"
#include "fam/fam_exception.h"
#include "pmi/fam_runtime.h"

namespace openfam {

// class fam_extras {

// public:
fam_extras::fam_extras(fam *faminp) { myfam = faminp; }
// SCATTER OP
void fam_extras::fam_queue_operation(FAM_QUEUE_OP op, void *local,
                                     Fam_Descriptor *descriptor,
                                     uint64_t nElements, uint64_t *elementIndex,
                                     uint64_t elementSize) {
  // std::cout<<"fam_queue_operation SCATTER "<<std::endl;
  myfam->myprint();
#if 0
        fam::Impl_ *famImpl_ = myfam->pimpl_;
        famImpl_->myprint();
        std::cout<<myfam->pimpl_<<std::endl;
#endif
  return;
}

// ADD, GET, PUT
void fam_extras::fam_queue_operation(FAM_QUEUE_OP op,
                                     Fam_Descriptor *descriptor, int32_t value,
                                     uint64_t elementIndex) {
  // std::cout<<"fam_queue_operation ADD int32 "<<std::endl;
  myfam->fam_queue_operation(op, descriptor, value, elementIndex);
#if 0
        myfam->pimpl_->myprint();
#endif
  return;
}

void fam_extras::fam_queue_operation(FAM_QUEUE_OP op,
                                     Fam_Descriptor *descriptor, int64_t value,
                                     uint64_t elementIndex) {
  // std::cout<<"fam_queue_operation ADD int64 "<<std::endl;
  myfam->myprint();
#if 0
        myfam->pimpl_->myprint();
#endif
  return;
}
void fam_extras::fam_aggregate_flush(Fam_Descriptor *descriptor) {
  myfam->fam_aggregate_flush(descriptor);
}
//}
}
