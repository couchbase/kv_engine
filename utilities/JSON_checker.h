/* JSON_checker.h */

#include <memcached/extension.h>

typedef struct JSON_checker_struct {
    int state;
    int depth;
    int top;
    int* stack;
} * JSON_checker;

#ifdef __cplusplus
extern "C" {
#endif
MEMCACHED_PUBLIC_API int checkUTF8JSON(const unsigned char* data, size_t size);
#ifdef __cplusplus
}
#endif
