/*
 *  * 
 *   * */
#ifndef _MY_TIME_H_
#define _MY_TIME_H_

#include <chrono>


using namespace std;
using namespace chrono;

using My_Time = uint64_t; 


   static inline My_Time get_my_time() {
#if 1
        long int time = static_cast<long int>(
            duration_cast<nanoseconds>(
                high_resolution_clock::now().time_since_epoch())
                .count());
        return time;
#else // using intel tsc
        uint64_t hi, lo, aux;
        __asm__ __volatile__("rdtscp" : "=a"(lo), "=d"(hi), "=c"(aux));
        return (uint64_t)lo | ((uint64_t)hi << 32);
#endif
    }

    static inline uint64_t my_time_diff_nanoseconds(My_Time start, My_Time end) {
        return (end - start);
}
#endif // _My_TIME_H_
