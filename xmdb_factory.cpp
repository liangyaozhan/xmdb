/*
 * lmdb_factory.cpp
 *
 *  Created on: Jul 11, 2017
 */

#include "xmdb.h"


xmdb* xmdb::get_instance(int index)
{
    const char *paths[] = {
            "testdb",
            "lmdb02",
            "lmdb03",
            "lmdb04",
            "lmdb05",
            "lmdb06",
            "lmdb07",
            "lmdb08",
            "lmdb09",
            "lmdb10",
            "lmdb11",
            "lmdb12",
            "lmdb13",
            "lmdb14",
            "lmdb15",
            "lmdb16",
    };

    static xmdb *p[16];
    if (p[index] == 0) {
        p[index] = new xmdb(paths[index]);
    }
    return p[index];
}







