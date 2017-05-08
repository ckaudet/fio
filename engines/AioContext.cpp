/*
 * AioContextEngine wrapping C++ AioContext class
 *
 * To build it run below command in fio top directory:
 * g++ -Wall -std=gnu++11 -I/path/to/fbcode -pthread -DFIO_EXTERNAL_ENGINE -O2 \
 *    -g -shared -rdynamic -fPIC -o engines/AioContext engines/AioContext.cpp
 */
#include "../fio.h"

#include <chrono>
#include <condition_variable>
#include <mutex>

#include "lithium/experimental/nvm/Utils.h"
#include "lithium/experimental/nvm/AioContext.h"

using namespace facebook::lithium::experimental;
using namespace std::chrono;

class AioContextEngine : public AioThreadSwitcher {
public:
  AioContextEngine(size_t maxPending, size_t queueSize)
      : done_(queueSize), ctx_(*this, maxPending, queueSize), waitingFor_(0) {}

  static AioContextEngine *get(struct thread_data *td) {
    AIO_CHECK(td);
    return reinterpret_cast<AioContextEngine *>(td->io_ops_data);
  }

  int getevents(unsigned int min, unsigned int max, const struct timespec *t) {
    const nanoseconds timeout(t ? (1000 * t->tv_sec + t->tv_nsec) : -1);

    std::unique_lock<std::mutex> lock(mutex_);
    while (done_.size() < min) {
      waitingFor_ = min;
      cond_.wait_until(lock, steady_clock::now() + timeout);
    }
    waitingFor_ = 0;
    return done_.size() > max ? max : done_.size();
  }

  struct io_u *getCompleted() {
    mutex_.lock();
    AIO_CHECK(!done_.isEmpty());
    RequestWithResult rwb = done_.read();
    mutex_.unlock();

    struct io_u *io_u = reinterpret_cast<struct io_u*>(rwb.req.userData);
    AIO_CHECK(io_u);

    const int64_t xfer_buflen = static_cast<int64_t>(io_u->xfer_buflen);
    if (rwb.bytes != xfer_buflen) {
      if (rwb.bytes > xfer_buflen) {
        io_u->error = -rwb.bytes;
      } else {
        io_u->resid = io_u->xfer_buflen - rwb.bytes;
      }
    } else {
      io_u->error = 0;
    }

    return io_u;
  }

  int queue(struct thread_data *td, struct io_u *io_u) {
    AIO_CHECK(io_u);
    AIO_CHECK(io_u->ddir == DDIR_READ || io_u->ddir == DDIR_WRITE);

    AioRequestType type =
        io_u->ddir == DDIR_READ ? AioRequestType::READ : AioRequestType::WRITE;
    AioRequest req(io_u->file->fd, io_u->xfer_buf, io_u->xfer_buflen,
                   io_u->offset, type, io_u);
    bool ok = ctx_.submitRequest(std::move(req), nullptr);
    return ok ? FIO_Q_QUEUED : FIO_Q_BUSY;
  }

  void callbacksBatchStart() {
    mutex_.lock();
    AIO_CHECK(!batchStarted_);
    batchStarted_ = true;
  }

  void callbacksBatchEnd() {
    AIO_CHECK(batchStarted_);
    batchStarted_ = false;
    if (waitingFor_ && done_.size() >= waitingFor_) {
      cond_.notify_one();
    }
    mutex_.unlock();
  }

  void callDoneInUserThread(AioCallback *, AioRequest &&req,
                            int64_t bytes) override {
    done_.write(RequestWithResult{std::move(req), bytes});
  }

  void callDroppedInUserThread(AioCallback *, AioRequest &&) override {}

private:
  struct RequestWithResult {
    RequestWithResult(AioRequest &&r, int64_t b)
        : req(std::move(r)), bytes(b) {}

    RequestWithResult(RequestWithResult &&r)
        : req(std::move(r.req)), bytes(r.bytes) {}

    AioRequest req;
    int64_t bytes;
  };
  std::mutex mutex_;
  std::condition_variable cond_;
  RingBuffer<RequestWithResult> done_;
  AioContext ctx_;
  size_t waitingFor_;
  bool batchStarted_ = false;
  char* arr[128];
};

extern "C" {

static struct io_u *aioctx_event(struct thread_data *td, int /* event_idx */)
{
  auto *engine = AioContextEngine::get(td);
  AIO_CHECK(engine);
  return engine->getCompleted();
}

static int aioctx_getevents(struct thread_data *td, unsigned int min,
                            unsigned int max, const struct timespec *t) {
  auto* engine = AioContextEngine::get(td);
  AIO_CHECK(engine);
	unsigned actual_min = td->o.iodepth_batch_complete_min == 0 ? 0 : min;
  return engine->getevents(actual_min, max, t);
}

static int aioctx_queue(struct thread_data *td, struct io_u *io_u)
{
	fio_ro_check(td, io_u);
  auto* engine = AioContextEngine::get(td);
  AIO_CHECK(engine);
  return engine->queue(td, io_u);
}

static void aioctx_cleanup(struct thread_data *td)
{
  if (auto* engine = AioContextEngine::get(td)) {
    delete engine;
  }
}

static int aioctx_init(struct thread_data *td)
{
//  td->io_ops_data = new AioContextEngine(td->o.iodepth, td->o.iodepth);
  td->io_ops_data = new AioContextEngine(32, 4096);
	return 0;
}

static struct ioengine_ops ioengine;

void get_ioengine(struct ioengine_ops **ioengine_ptr)
{
	*ioengine_ptr = &ioengine;

	ioengine.name           = "AioContext";
	ioengine.version        = FIO_IOOPS_VERSION;
	ioengine.queue          = aioctx_queue;
	ioengine.getevents      = aioctx_getevents;
	ioengine.event          = aioctx_event;
	ioengine.init           = aioctx_init;
	ioengine.cleanup        = aioctx_cleanup;
	ioengine.open_file		  = generic_open_file;
	ioengine.close_file		  = generic_close_file;
	ioengine.get_file_size  = generic_get_file_size;
}
}
