#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <thread>

#define CLOCKFD 3
#define FD_TO_CLOCKID(fd) ((~(clockid_t)(fd) << 3) | CLOCKFD)
#define CLOCKID_TO_FD(clk) ((unsigned int)~((clk) >> 3))

int main() {
  int fd = open("/dev/ptp0", O_RDWR);
  if (fd < 0) {
    perror("open");
    return 1;
  }

  int clkid = FD_TO_CLOCKID(fd);

  for (int i = 0; i < 10; ++i) {
    struct timespec ts;
    clock_gettime(clkid, &ts);
    printf("clock_gettime: %ld.%08ld\n", ts.tv_sec, ts.tv_nsec);

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
  close(fd);
  return 0;
}
