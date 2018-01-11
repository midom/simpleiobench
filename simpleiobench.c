/*
Copyright 2017 Domas Mituzas, Facebook

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

#include <glib.h>

int numthreads = 32;
size_t arenasize = 1;  /* Gigabytes */
size_t blocksize = 16; /* Kilobytes  */
int interval = 1;
int count = 0;
int nblocks = 1;
int buf0fered = 0;

gboolean buffered = FALSE;
gboolean writing = FALSE;
gboolean human = FALSE;
gboolean should_sync = FALSE;

size_t align = 4096;

static GOptionEntry entries[] = {
    {"threads", 't', 0, G_OPTION_ARG_INT, &numthreads,
     "Number of threads to run", "N"},
    {"size", 's', 0, G_OPTION_ARG_INT, &arenasize, "Arena size, in gigabytes",
     "GB"},
    {"block", 'b', 0, G_OPTION_ARG_INT, &blocksize, "Block size, in kilobytes",
     "KB"},
    {"nblocks", 'n', 0, G_OPTION_ARG_INT, &nblocks,
     "Number of blocks to read sequentially", "N"},
    {"interval", 'i', 0, G_OPTION_ARG_INT, &interval,
     "Reporting interval, in seconds", "s"},
    {"count", 'c', 0, G_OPTION_ARG_INT, &count,
     "Number of intervals to run, default forever", "N"},
    {"human", 'h', 0, G_OPTION_ARG_NONE, &human, "Use more human output"},
    {"buffered", 'B', 0, G_OPTION_ARG_NONE, &buffered, "Use buffered I/O"},
    {"write", 'W', 0, G_OPTION_ARG_NONE, &writing, "Destructive write!"},
    {"sync", 'S', 0, G_OPTION_ARG_NONE, &should_sync, "fsync after write"},
    {NULL}};

/* Statistics */
gint ops = 0;
int fd;

double inline microtime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (double)tv.tv_sec + (double)tv.tv_usec / (1000 * 1000);
}

void *worker(void *x) {
  unsigned ctx = (unsigned)pthread_self();
  ssize_t r;

  char *data = (char *)malloc(blocksize + align);
  data += align - ((unsigned long)data % align);


  for (;;) {
    off_t offset = (rand_r(&ctx) * blocksize) % arenasize;
    *data = rand_r(&ctx);
    for (int seq = 0; seq < nblocks; seq++) {
      if (!writing) {
        r = pread64(fd, data, blocksize, offset + seq * blocksize);
      } else {
        r = pwrite64(fd, data, blocksize, offset + seq * blocksize);
        if (should_sync)
          fdatasync(fd);
      }

      if (r != blocksize) {
        fprintf(stderr, "Read less data (%ld) than requested (%lu)\n", r,
                blocksize);
        exit(1);
      }
      g_atomic_int_add(&ops, 1);
    }
  }
  return NULL;
}

int main(int ac, char **av) {
  pthread_t *threads;
  int i;

  GError *error = NULL;
  GOptionContext *context = g_option_context_new("FILE");
  g_option_context_add_main_entries(context, entries, NULL);
  g_option_context_set_summary(context,
                               "Simple I/O concurrent random benchmark");
  if (!g_option_context_parse(context, &ac, &av, &error)) {
    fprintf(stderr, "option parsing failed: %s\n", error->message);
    exit(1);
  }

  arenasize *= 1024 * 1024 * 1024L;
  blocksize *= 1024;

  if (ac != 2) {
    fprintf(stderr,
            "Please specify device or file name, check --help for usage\n");
    exit(1);
  }

  char *filename = av[1];
  fd = open(filename,
            (writing ? O_WRONLY : O_RDONLY) | (buffered ? 0 : O_DIRECT) |
                O_LARGEFILE,
            0644);
  if (fd < 0) {
    fprintf(stderr, "Couldn't open the file or device: %s\n", strerror(errno));
    exit(1);
  }

  threads = (pthread_t *)malloc(sizeof(pthread_t) * numthreads);
  for (i = numthreads; numthreads--;) {
    pthread_create(&threads[i], NULL, worker, NULL);
  }
  for (i = 0; !count || i < count; i++) {
    sleep(interval);
    if (human) {
      printf("%d %s %.f MB/s\n", ops / interval, (writing ? "writes" : "reads"),
             (double)(ops * blocksize / (interval * 1024 * 1024)));
    } else {
      printf("%d\n", ops / interval);
    }
    g_atomic_int_set(&ops, 0);
    fflush(stdout);
  }
  return 0;
}
