add_library(mcd_executor STATIC
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
            taskable.h
            cb3_taskqueue.cc
            cb3_taskqueue.h
            tasks.def.h
            workload.h
            )
target_include_directories(mcd_executor PRIVATE SYSTEM ${hdr_histogram_SOURCE_DIR}/src)
target_link_libraries(mcd_executor PUBLIC Folly::folly platform spdlog statistics)
cb_enable_unity_build(mcd_executor)
kv_enable_pch(mcd_executor)
