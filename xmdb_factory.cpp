/*
 * lmdb_factory.cpp
 *
 *  Created on: Jul 11, 2017
 */

#include "xmdb.h"

static const char *paths[] = {
        "lmdb-userinfo",
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

static xmdb *lmdb_handles[16];

xmdb* xmdb::get_instance(int index)
{
    if (lmdb_handles[index] == 0) {
        lmdb_handles[index] = new xmdb(paths[index]);
    }
    return lmdb_handles[index];
}


xmdb* xmdb::get_instance(const std::string &dbname)
{
    int index = -1;
    for (unsigned int i=0; i<sizeof(paths)/sizeof(paths[0]); ++i)
    {
        if ( paths[i] == dbname )
        {
            index = i;
            break;
        }
    }

    if ( index < 0 ){
        return 0;
    }

    if (lmdb_handles[index] == 0) {
        lmdb_handles[index] = new xmdb(paths[index]);
    }
    return lmdb_handles[index];
}

