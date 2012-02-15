/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#include "config.h"

#include <stdlib.h>
#include <string.h>

#include "sqlite-vfs.h"

/*
** An instance of this structure is attached to the each epstat VFS to
** provide auxiliary information.
*/
typedef struct vfsepstat_info vfsepstat_info;
struct vfsepstat_info {
    sqlite3_vfs *pRootVfs;              /* The underlying real VFS */
    const char *zVfsName;               /* Name of this epstat-VFS */
    struct vfs_callbacks cb;
    void *cbarg;                        /* Callback final argument */
    sqlite3_vfs *pEpstatVfs;             /* Pointer back to the epstat VFS */
};

/*
** The sqlite3_file object for the epstat VFS
*/
typedef struct vfsepstat_file vfsepstat_file;
struct vfsepstat_file {
    sqlite3_file base;        /* Base class.  Must be first */
    vfsepstat_info *pInfo;     /* The epstat-VFS to which this file belongs */
    const char *zFName;       /* Base name of the file */
    sqlite3_file *pReal;      /* The real underlying file */
    sqlite_int64 offset;      /* Current file offset */
};

/*
** Method declarations for vfsepstat_file.
*/
static int vfsepstatClose(sqlite3_file*);
static int vfsepstatRead(sqlite3_file*, void*, int iAmt, sqlite3_int64 iOfst);
static int vfsepstatWrite(sqlite3_file*,const void*,int iAmt, sqlite3_int64);
static int vfsepstatTruncate(sqlite3_file*, sqlite3_int64 size);
static int vfsepstatSync(sqlite3_file*, int flags);
static int vfsepstatFileSize(sqlite3_file*, sqlite3_int64 *pSize);
static int vfsepstatLock(sqlite3_file*, int);
static int vfsepstatUnlock(sqlite3_file*, int);
static int vfsepstatCheckReservedLock(sqlite3_file*, int *);
static int vfsepstatFileControl(sqlite3_file*, int op, void *pArg);
static int vfsepstatSectorSize(sqlite3_file*);
static int vfsepstatDeviceCharacteristics(sqlite3_file*);
static int vfsepstatShmLock(sqlite3_file*,int,int,int);
static int vfsepstatShmMap(sqlite3_file*,int,int,int, void volatile **);
static void vfsepstatShmBarrier(sqlite3_file*);
static int vfsepstatShmUnmap(sqlite3_file*,int);

/*
** Method declarations for vfsepstat_vfs.
*/
static int vfsepstatOpen(sqlite3_vfs*, const char *, sqlite3_file*, int , int *);
static int vfsepstatDelete(sqlite3_vfs*, const char *zName, int syncDir);
static int vfsepstatAccess(sqlite3_vfs*, const char *zName, int flags, int *);
static int vfsepstatFullPathname(sqlite3_vfs*, const char *zName, int, char *);
static void *vfsepstatDlOpen(sqlite3_vfs*, const char *zFilename);
static void vfsepstatDlError(sqlite3_vfs*, int nByte, char *zErrMsg);
static void (*vfsepstatDlSym(sqlite3_vfs*,void*, const char *zSymbol))(void);
static void vfsepstatDlClose(sqlite3_vfs*, void*);
static int vfsepstatRandomness(sqlite3_vfs*, int nByte, char *zOut);
static int vfsepstatSleep(sqlite3_vfs*, int microseconds);
static int vfsepstatCurrentTime(sqlite3_vfs*, double*);
static int vfsepstatGetLastError(sqlite3_vfs*, int, char*);
static int vfsepstatCurrentTimeInt64(sqlite3_vfs*, sqlite3_int64*);

/*
** Return a pointer to the tail of the pathname.  Examples:
**
**     /home/drh/xyzzy.txt -> xyzzy.txt
**     xyzzy.txt           -> xyzzy.txt
*/
static const char *fileTail(const char *z){
    int i;
    if( z==0 ) return 0;
    i = strlen(z)-1;
    while( i>0 && z[i-1]!='/' ){ i--; }
    return &z[i];
}

/*
** Close an vfsepstat-file.
*/
static int vfsepstatClose(sqlite3_file *pFile){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = p->pInfo;
    int rc = p->pReal->pMethods->xClose(p->pReal);
    if( rc==SQLITE_OK ){
        sqlite3_free((void*)p->base.pMethods);
        p->base.pMethods = 0;
    }
    pInfo->cb.gotClose(rc, pInfo->cbarg);
    return rc;
}

/*
** Read data from an vfsepstat-file.
*/
static int vfsepstatRead(
                        sqlite3_file *pFile,
                        void *zBuf,
                        int iAmt,
                        sqlite_int64 iOfst
                        ){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = p->pInfo;
    hrtime_t start = gethrtime();
    int rc = p->pReal->pMethods->xRead(p->pReal, zBuf, iAmt, iOfst);
    hrtime_t end = gethrtime();
    sqlite_int64 old_offset = p->offset;
    p->offset = iOfst + iAmt;
    pInfo->cb.gotRead(rc, iAmt, p->offset - old_offset,
                      end - start, pInfo->cbarg);
    return rc;
}

/*
** Write data to an vfsepstat-file.
*/
static int vfsepstatWrite(
                         sqlite3_file *pFile,
                         const void *zBuf,
                         int iAmt,
                         sqlite_int64 iOfst
                         ){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = p->pInfo;
    hrtime_t start = gethrtime();
    int rc = p->pReal->pMethods->xWrite(p->pReal, zBuf, iAmt, iOfst);
    hrtime_t end = gethrtime();
    sqlite_int64 old_offset = p->offset;
    p->offset = iOfst + iAmt;
    pInfo->cb.gotWrite(rc, iAmt, p->offset - old_offset,
                       end - start, pInfo->cbarg);
    return rc;
}

/*
** Truncate an vfsepstat-file.
*/
static int vfsepstatTruncate(sqlite3_file *pFile, sqlite_int64 size){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = p->pInfo;
    int rc = p->pReal->pMethods->xTruncate(p->pReal, size);
    pInfo->cb.gotTrunc(rc, pInfo->cbarg);
    return rc;
}

/*
** Sync an vfsepstat-file.
*/
static int vfsepstatSync(sqlite3_file *pFile, int flags){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = p->pInfo;
    hrtime_t start = gethrtime();
    int rc = p->pReal->pMethods->xSync(p->pReal, flags);
    hrtime_t end = gethrtime();
    pInfo->cb.gotSync(rc, flags, end - start, pInfo->cbarg);
    return rc;
}

/*
** Return the current file-size of an vfsepstat-file.
*/
static int vfsepstatFileSize(sqlite3_file *pFile, sqlite_int64 *pSize){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    int rc = p->pReal->pMethods->xFileSize(p->pReal, pSize);
    return rc;
}

/*
** Lock an vfsepstat-file.
*/
static int vfsepstatLock(sqlite3_file *pFile, int eLock){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = p->pInfo;
    int rc = p->pReal->pMethods->xLock(p->pReal, eLock);
    pInfo->cb.gotLock(rc, pInfo->cbarg);
    return rc;
}

/*
** Unlock an vfsepstat-file.
*/
static int vfsepstatUnlock(sqlite3_file *pFile, int eLock){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    // vfsepstat_info *pInfo = p->pInfo;
    int rc = p->pReal->pMethods->xUnlock(p->pReal, eLock);
    return rc;
}

/*
** Check if another file-handle holds a RESERVED lock on an vfsepstat-file.
*/
static int vfsepstatCheckReservedLock(sqlite3_file *pFile, int *pResOut){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    int rc = p->pReal->pMethods->xCheckReservedLock(p->pReal, pResOut);
    return rc;
}

/*
** File control method. For custom operations on an vfsepstat-file.
*/
static int vfsepstatFileControl(sqlite3_file *pFile, int op, void *pArg){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    int rc = p->pReal->pMethods->xFileControl(p->pReal, op, pArg);
    return rc;
}

/*
** Return the sector-size in bytes for an vfsepstat-file.
*/
static int vfsepstatSectorSize(sqlite3_file *pFile){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = p->pInfo;
    int rc = p->pReal->pMethods->xSectorSize(p->pReal);
    pInfo->cb.gotSectorSize(rc, pInfo->cbarg);
    return rc;
}

/*
** Return the device characteristic flags supported by an vfsepstat-file.
*/
static int vfsepstatDeviceCharacteristics(sqlite3_file *pFile){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    int rc = p->pReal->pMethods->xDeviceCharacteristics(p->pReal);
    return rc;
}

/*
** Shared-memory operations.
*/
static int vfsepstatShmLock(sqlite3_file *pFile, int ofst, int n, int flags){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    int rc = p->pReal->pMethods->xShmLock(p->pReal, ofst, n, flags);
    return rc;
}
static int vfsepstatShmMap(
                          sqlite3_file *pFile,
                          int iRegion,
                          int szRegion,
                          int isWrite,
                          void volatile **pp
                          ){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    return p->pReal->pMethods->xShmMap(p->pReal, iRegion, szRegion, isWrite, pp);
}
static void vfsepstatShmBarrier(sqlite3_file *pFile){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    p->pReal->pMethods->xShmBarrier(p->pReal);
}
static int vfsepstatShmUnmap(sqlite3_file *pFile, int delFlag){
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    return p->pReal->pMethods->xShmUnmap(p->pReal, delFlag);
}



/*
** Open an vfsepstat file handle.
*/
static int vfsepstatOpen(
                        sqlite3_vfs *pVfs,
                        const char *zName,
                        sqlite3_file *pFile,
                        int flags,
                        int *pOutFlags
                        ){
    int rc;
    vfsepstat_file *p = (vfsepstat_file *)pFile;
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    p->pInfo = pInfo;
    p->zFName = zName ? fileTail(zName) : "<temp>";
    p->pReal = (sqlite3_file *)&p[1];
    p->offset = 0;
    rc = pRoot->xOpen(pRoot, zName, p->pReal, flags, pOutFlags);
    if( p->pReal->pMethods ){
        sqlite3_io_methods *pNew = sqlite3_malloc( sizeof(*pNew) );
        const sqlite3_io_methods *pSub = p->pReal->pMethods;
        memset(pNew, 0, sizeof(*pNew));
        pNew->iVersion = pSub->iVersion;
        pNew->xClose = vfsepstatClose;
        pNew->xRead = vfsepstatRead;
        pNew->xWrite = vfsepstatWrite;
        pNew->xTruncate = vfsepstatTruncate;
        pNew->xSync = vfsepstatSync;
        pNew->xFileSize = vfsepstatFileSize;
        pNew->xLock = vfsepstatLock;
        pNew->xUnlock = vfsepstatUnlock;
        pNew->xCheckReservedLock = vfsepstatCheckReservedLock;
        pNew->xFileControl = vfsepstatFileControl;
        pNew->xSectorSize = vfsepstatSectorSize;
        pNew->xDeviceCharacteristics = vfsepstatDeviceCharacteristics;
        if( pNew->iVersion>=2 ){
            pNew->xShmMap = pSub->xShmMap ? vfsepstatShmMap : 0;
            pNew->xShmLock = pSub->xShmLock ? vfsepstatShmLock : 0;
            pNew->xShmBarrier = pSub->xShmBarrier ? vfsepstatShmBarrier : 0;
            pNew->xShmUnmap = pSub->xShmUnmap ? vfsepstatShmUnmap : 0;
        }
        pFile->pMethods = pNew;
    }
    pInfo->cb.gotOpen(rc, pInfo->cbarg);
    return rc;
}

/*
** Delete the file located at zPath. If the dirSync argument is true,
** ensure the file-system modifications are synced to disk before
** returning.
*/
static int vfsepstatDelete(sqlite3_vfs *pVfs, const char *zPath, int dirSync){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    hrtime_t starttime = gethrtime();
    int rc = pRoot->xDelete(pRoot, zPath, dirSync);
    hrtime_t endtime = gethrtime();
    pInfo->cb.gotDelete(rc, endtime - starttime, pInfo->cbarg);
    return rc;
}

/*
** Test for access permissions. Return true if the requested permission
** is available, or false otherwise.
*/
static int vfsepstatAccess(
                          sqlite3_vfs *pVfs,
                          const char *zPath,
                          int flags,
                          int *pResOut
                          ){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xAccess(pRoot, zPath, flags, pResOut);
}

/*
** Populate buffer zOut with the full canonical pathname corresponding
** to the pathname in zPath. zOut is guaranteed to point to a buffer
** of at least (DEVSYM_MAX_PATHNAME+1) bytes.
*/
static int vfsepstatFullPathname(
                                sqlite3_vfs *pVfs,
                                const char *zPath,
                                int nOut,
                                char *zOut
                                ){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xFullPathname(pRoot, zPath, nOut, zOut);
}

/*
** Open the dynamic library located at zPath and return a handle.
*/
static void *vfsepstatDlOpen(sqlite3_vfs *pVfs, const char *zPath){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xDlOpen(pRoot, zPath);
}

/*
** Populate the buffer zErrMsg (size nByte bytes) with a human readable
** utf-8 string describing the most recent error encountered associated
** with dynamic libraries.
*/
static void vfsepstatDlError(sqlite3_vfs *pVfs, int nByte, char *zErrMsg){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    pRoot->xDlError(pRoot, nByte, zErrMsg);
}

/*
** Return a pointer to the symbol zSymbol in the dynamic library pHandle.
*/
static void (*vfsepstatDlSym(sqlite3_vfs *pVfs,void *p,const char *zSym))(void){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xDlSym(pRoot, p, zSym);
}

/*
** Close the dynamic library handle pHandle.
*/
static void vfsepstatDlClose(sqlite3_vfs *pVfs, void *pHandle){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    pRoot->xDlClose(pRoot, pHandle);
}

/*
** Populate the buffer pointed to by zBufOut with nByte bytes of
** random data.
*/
static int vfsepstatRandomness(sqlite3_vfs *pVfs, int nByte, char *zBufOut){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xRandomness(pRoot, nByte, zBufOut);
}

/*
** Sleep for nMicro microseconds. Return the number of microseconds
** actually slept.
*/
static int vfsepstatSleep(sqlite3_vfs *pVfs, int nMicro){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xSleep(pRoot, nMicro);
}

/*
** Return the current time as a Julian Day number in *pTimeOut.
*/
static int vfsepstatCurrentTime(sqlite3_vfs *pVfs, double *pTimeOut){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xCurrentTime(pRoot, pTimeOut);
}
static int vfsepstatCurrentTimeInt64(sqlite3_vfs *pVfs, sqlite3_int64 *pTimeOut){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xCurrentTimeInt64(pRoot, pTimeOut);
}

/*
** Return th3 emost recent error code and message
*/
static int vfsepstatGetLastError(sqlite3_vfs *pVfs, int iErr, char *zErr){
    vfsepstat_info *pInfo = (vfsepstat_info*)pVfs->pAppData;
    sqlite3_vfs *pRoot = pInfo->pRootVfs;
    return pRoot->xGetLastError(pRoot, iErr, zErr);
}


/*
** Clients invoke this routine to construct a new epstat-vfs shim.
**
** Return SQLITE_OK on success.
**
** SQLITE_NOMEM is returned in the case of a memory allocation error.
** SQLITE_NOTFOUND is returned if zOldVfsName does not exist.
*/
int vfsepstat_register(
                      const char *zEpstatName,
                      const char *zOldVfsName,
                      struct vfs_callbacks cb,
                      void *cbarg
                      ){
    sqlite3_vfs *pNew;
    sqlite3_vfs *pRoot;
    vfsepstat_info *pInfo;
    int nName;
    int nByte;

    pRoot = sqlite3_vfs_find(zOldVfsName);
    if( pRoot==0 ) return SQLITE_NOTFOUND;
    nName = strlen(zEpstatName);
    nByte = sizeof(*pNew) + sizeof(*pInfo) + nName + 1;
    pNew = sqlite3_malloc( nByte );
    if( pNew==0 ) return SQLITE_NOMEM;
    memset(pNew, 0, nByte);
    pInfo = (vfsepstat_info*)&pNew[1];
    pNew->iVersion = pRoot->iVersion;
    pNew->szOsFile = pRoot->szOsFile + sizeof(vfsepstat_file);
    pNew->mxPathname = pRoot->mxPathname;
    pNew->zName = (char*)&pInfo[1];
    memcpy((char*)&pInfo[1], zEpstatName, nName+1);
    pNew->pAppData = pInfo;
    pNew->xOpen = vfsepstatOpen;
    pNew->xDelete = vfsepstatDelete;
    pNew->xAccess = vfsepstatAccess;
    pNew->xFullPathname = vfsepstatFullPathname;
    pNew->xDlOpen = pRoot->xDlOpen==0 ? 0 : vfsepstatDlOpen;
    pNew->xDlError = pRoot->xDlError==0 ? 0 : vfsepstatDlError;
    pNew->xDlSym = pRoot->xDlSym==0 ? 0 : vfsepstatDlSym;
    pNew->xDlClose = pRoot->xDlClose==0 ? 0 : vfsepstatDlClose;
    pNew->xRandomness = vfsepstatRandomness;
    pNew->xSleep = vfsepstatSleep;
    pNew->xCurrentTime = vfsepstatCurrentTime;
    pNew->xGetLastError = pRoot->xGetLastError==0 ? 0 : vfsepstatGetLastError;
    if( pNew->iVersion>=2 ){
        pNew->xCurrentTimeInt64 = pRoot->xCurrentTimeInt64==0 ? 0 :
            vfsepstatCurrentTimeInt64;
    }
    pInfo->pRootVfs = pRoot;
    pInfo->cb = cb;
    pInfo->cbarg = cbarg;
    pInfo->zVfsName = pNew->zName;
    pInfo->pEpstatVfs = pNew;
    return sqlite3_vfs_register(pNew, 0);
}
