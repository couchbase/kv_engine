/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
#pragma once

#ifdef WIN32
// 'declaration' : no matching operator delete found; memory will not be
// freed if initialization throws an exception
#pragma warning(disable : 4291)
// 'conversion' conversion from 'type1' to 'type2', possible loss of data
#pragma warning(disable : 4244)
// 'var' : conversion from 'size_t' to 'type', possible loss of data
#pragma warning(disable : 4267)
// Turn of deprecated warnings
#pragma warning(disable : 4996)

#endif // WIN32
