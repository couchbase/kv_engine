#! /bin/bash

for f in `find . -name "*.cc"` `find . -name "*.h"`
do
    echo $f
    sed -e s,protocol_binary_response_status,cb::mcbp::Status,g $f | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUCCESS,cb::mcbp::Status::Success,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_KEY_ENOENT,cb::mcbp::Status::KeyEnoent,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_KEY_EEXISTS,cb::mcbp::Status::KeyEexists,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_E2BIG,cb::mcbp::Status::E2big,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_EINVAL,cb::mcbp::Status::Einval,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_NOT_STORED,cb::mcbp::Status::NotStored,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_DELTA_BADVAL,cb::mcbp::Status::DeltaBadval,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_NOT_MY_VBUCKET,cb::mcbp::Status::NotMyVbucket,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_NO_BUCKET,cb::mcbp::Status::NoBucket,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_LOCKED,cb::mcbp::Status::Locked,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_AUTH_STALE,cb::mcbp::Status::AuthStale,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_AUTH_ERROR,cb::mcbp::Status::AuthError,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_AUTH_CONTINUE,cb::mcbp::Status::AuthContinue,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_ERANGE,cb::mcbp::Status::Erange,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_ROLLBACK,cb::mcbp::Status::Rollback,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_EACCESS,cb::mcbp::Status::Eaccess,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_NOT_INITIALIZED,cb::mcbp::Status::NotInitialized,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_UNKNOWN_COMMAND,cb::mcbp::Status::UnknownCommand,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_ENOMEM,cb::mcbp::Status::Enomem,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_NOT_SUPPORTED,cb::mcbp::Status::NotSupported,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_EINTERNAL,cb::mcbp::Status::Einternal,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_EBUSY,cb::mcbp::Status::Ebusy,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_ETMPFAIL,cb::mcbp::Status::Etmpfail,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_XATTR_EINVAL,cb::mcbp::Status::XattrEinval,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_UNKNOWN_COLLECTION,cb::mcbp::Status::UnknownCollection,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_NO_COLLECTIONS_MANIFEST,cb::mcbp::Status::NoCollectionsManifest,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_CANNOT_APPLY_COLLECTIONS_MANIFEST,cb::mcbp::Status::CannotApplyCollectionsManifest,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_COLLECTIONS_MANIFEST_IS_AHEAD,cb::mcbp::Status::CollectionsManifestIsAhead,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_ENOENT,cb::mcbp::Status::SubdocPathEnoent,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_MISMATCH,cb::mcbp::Status::SubdocPathMismatch,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EINVAL,cb::mcbp::Status::SubdocPathEinval,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_E2BIG,cb::mcbp::Status::SubdocPathE2big,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_E2DEEP,cb::mcbp::Status::SubdocDocE2deep,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_CANTINSERT,cb::mcbp::Status::SubdocValueCantinsert,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_DOC_NOTJSON,cb::mcbp::Status::SubdocDocNotJson,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_NUM_ERANGE,cb::mcbp::Status::SubdocNumErange,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_DELTA_EINVAL,cb::mcbp::Status::SubdocDeltaEinval,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_PATH_EEXISTS,cb::mcbp::Status::SubdocPathEexists,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_VALUE_ETOODEEP,cb::mcbp::Status::SubdocValueEtoodeep,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_COMBO,cb::mcbp::Status::SubdocInvalidCombo,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE,cb::mcbp::Status::SubdocMultiPathFailure,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_SUCCESS_DELETED,cb::mcbp::Status::SubdocSuccessDeleted,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_INVALID_FLAG_COMBO,cb::mcbp::Status::SubdocXattrInvalidFlagCombo,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_INVALID_KEY_COMBO,cb::mcbp::Status::SubdocXattrInvalidKeyCombo,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_UNKNOWN_MACRO,cb::mcbp::Status::SubdocXattrUnknownMacro,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_UNKNOWN_VATTR,cb::mcbp::Status::SubdocXattrUnknownVattr,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_XATTR_CANT_MODIFY_VATTR,cb::mcbp::Status::SubdocXattrCantModifyVattr,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_MULTI_PATH_FAILURE_DELETED,cb::mcbp::Status::SubdocMultiPathFailureDeleted,g | \
    sed -e s,PROTOCOL_BINARY_RESPONSE_SUBDOC_INVALID_XATTR_ORDER,cb::mcbp::Status::SubdocInvalidXattrOrder,g > $f.$$
    mv $f.$$ $f
done
