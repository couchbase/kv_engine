add_library(mcd_executor STATIC
            cancellable_cpu_executor.cc
            cb3_executorpool.cc
            cb3_executorpool.h
            cb3_executorthread.cc
            cb3_executorthread.h
            executorpool.cc
            executorpool.h
            fake_executorpool.h
            folly_executorpool.cc
            folly_executorpool.h
            futurequeue.h
            globaltask.cc
            globaltask.h
            mock_executor_pool.cc
            mock_executor_pool.h
            notifiable_task.cc
            taskable.h
            cb3_taskqueue.cc
            cb3_taskqueue.h
            tasks.def.h
            workload.h)
target_link_libraries(mcd_executor PUBLIC Folly::folly hdrhistogram platform spdlog::spdlog statistics)
cb_enable_unity_build(mcd_executor)
kv_enable_pch(mcd_executor)
