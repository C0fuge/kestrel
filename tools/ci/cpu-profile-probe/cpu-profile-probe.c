#define _GNU_SOURCE
#include <jni.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/syscall.h>

static pthread_t busy_thread;
static pthread_t wait_thread;
static volatile uint64_t sink;
static int busy_tid;
static int wait_tid;
static uint64_t busy_cpu;
static uint64_t wait_cpu;

static uint64_t nanos(clockid_t clock) {
    struct timespec ts;
    if (clock_gettime(clock, &ts)) abort();
    return (uint64_t)ts.tv_sec * 1000000000ULL + ts.tv_nsec;
}

/* Exported, uninlined names make native worker coverage directly testable. */
__attribute__((noinline, visibility("default")))
void ap_native_busy(void) {
    uint64_t end = nanos(CLOCK_THREAD_CPUTIME_ID) + 800000000ULL;
    uint64_t value = 1;
    while (nanos(CLOCK_THREAD_CPUTIME_ID) < end) {
        for (int i = 0; i < 8192; i++) value = value * 6364136223846793005ULL + 1;
        sink = value;
    }
}

__attribute__((noinline, visibility("default")))
void ap_native_wait(void) {
    struct timespec remaining = {2, 0};
    while (nanosleep(&remaining, &remaining)) {}
}

static void* busy(void* ignored) {
    (void)ignored;
    pthread_setname_np(pthread_self(), "ap-native-busy");
    busy_tid = (int)syscall(SYS_gettid);
    uint64_t start = nanos(CLOCK_THREAD_CPUTIME_ID);
    ap_native_busy();
    busy_cpu = nanos(CLOCK_THREAD_CPUTIME_ID) - start;
    return NULL;
}

static void* waiting(void* ignored) {
    (void)ignored;
    pthread_setname_np(pthread_self(), "ap-native-wait");
    wait_tid = (int)syscall(SYS_gettid);
    uint64_t start = nanos(CLOCK_THREAD_CPUTIME_ID);
    ap_native_wait();
    wait_cpu = nanos(CLOCK_THREAD_CPUTIME_ID) - start;
    return NULL;
}

JNIEXPORT void JNICALL Java_CpuProfileProbe_startNative(JNIEnv* env, jclass type) {
    (void)env;
    (void)type;
    if (pthread_create(&busy_thread, NULL, busy, NULL)
            || pthread_create(&wait_thread, NULL, waiting, NULL)) abort();
}

JNIEXPORT void JNICALL Java_CpuProfileProbe_finishNative(JNIEnv* env, jclass type) {
    (void)env;
    (void)type;
    if (pthread_join(busy_thread, NULL) || pthread_join(wait_thread, NULL)) abort();
    printf("native_busy_tid=%d\nnative_wait_tid=%d\nnative_busy_cpu_ns=%llu\n"
           "native_wait_cpu_ns=%llu\n", busy_tid, wait_tid,
           (unsigned long long)busy_cpu, (unsigned long long)wait_cpu);
    fflush(stdout);
}

JNIEXPORT jlong JNICALL Java_CpuProfileProbe_monotonicClock(JNIEnv* env, jclass type) {
    (void)env;
    (void)type;
    return (jlong)nanos(CLOCK_MONOTONIC);
}
