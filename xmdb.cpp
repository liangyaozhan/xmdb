/*
 * database_center.cpp
 *
 *  Created on: Jul 8, 2017
 *      Author: lyzh
 */

#include "xmdb.h"

xmdb::xmdb(const char* _path) :
        path(_path), dbenv(0), dbi(0), txn(0)
{
    init_lmdb();
}

xmdb::trans xmdb::transaction_begin_rw()
{
    MDB_txn *t = 0;
    mdb_txn_begin(dbenv, NULL, 0, &t);
    return trans(dbi, t);
}

xmdb::trans xmdb::transaction_begin_rd()
{
    MDB_txn *t = 0;
    mdb_txn_begin(dbenv, NULL, MDB_RDONLY, &t);
    return trans(dbi, t);
}

void xmdb::init_lmdb()
{
    (mdb_env_create(&dbenv));
    (mdb_env_set_maxreaders(dbenv, 32));
    (mdb_env_set_mapsize(dbenv, 10485760));
    (mdb_env_open(dbenv, path, MDB_FIXEDMAP /*|MDB_NOSYNC*/, 0664));
    (mdb_txn_begin(dbenv, NULL, 0, &txn));
    (mdb_dbi_open(txn, NULL, 0, &dbi));
    (mdb_txn_commit(txn));
    (mdb_env_stat(dbenv, &mst));
}

xmdb::~xmdb()
{
    mdb_dbi_close(dbenv, dbi);
    mdb_env_close(dbenv);
}

void xmdb::trans::abort()
{
    mdb_txn_abort(txn.get());
}

bool xmdb::trans::commit()
{
    bool ret = (0 == mdb_txn_commit(txn.get()));
    return ret;
}

bool xmdb::trans::get_value(const std::string& key, std::string& value)
{
    MDB_val k, v;
    char bufv[1024];
    k.mv_data = const_cast<char*>(key.c_str());
    k.mv_size = key.length();
    memset(bufv, 0x00, sizeof(bufv));
    v.mv_data = bufv;
    v.mv_size = sizeof(bufv) - 1;
    bool ret = (0 == mdb_get(txn.get(), dbi, &k, &v));
    if (ret)
    {
        value = std::string((char*) v.mv_data);
    }
    else
    {
        value = "";
    }
    return ret;
}

bool xmdb::trans::put_if_not_exist(const std::string& key,
        const std::string& value)
{
    MDB_val k, v;
    k.mv_data = const_cast<char*>(key.c_str());
    k.mv_size = key.length();
    v.mv_data = const_cast<char*>(value.c_str());
    v.mv_size = value.length();
    bool ret = (0 == mdb_put(txn.get(), dbi, &k, &v, MDB_NOOVERWRITE));
    return ret;
}

bool xmdb::trans::put(const std::string& key, const std::string& value)
{
    MDB_val k, v;
    k.mv_data = const_cast<char*>(key.c_str());
    k.mv_size = key.length();
    v.mv_data = const_cast<char*>(value.c_str());
    v.mv_size = value.length();
    bool ret = (0 == mdb_put(txn.get(), dbi, &k, &v, 0));
    return ret;
}

xmdb::cursor xmdb::trans::cursor_open()
{
    MDB_cursor *c = 0;
    mdb_cursor_open(txn.get(), dbi, &c);
    return cursor(c);
}

MDB_dbi xmdb::trans::db() const
{
    return this->dbi;
}

xmdb::trans& xmdb::trans::operator =(const trans& o)
{
    this->txn = o.txn;
    this->dbi = o.dbi;
    return *this;
}

xmdb::cursor::cursor()
{
}

xmdb::trans::trans(MDB_dbi d, MDB_txn* t) :
        txn(std::shared_ptr<MDB_txn>(t, [t](void*p)
        {   mdb_txn_abort(t);})), dbi(d)
{
}

bool xmdb::cursor::put(const std::string& key, int32_t value)
{
    return put(key, (const char*) &value, sizeof(value));
}

bool xmdb::cursor::put(const std::string& key, const char* data, int len)
{
    MDB_val k, v;
    k.mv_data = const_cast<char*>(key.c_str());
    k.mv_size = key.length();
    v.mv_data = const_cast<char*>(data);
    v.mv_size = len;
    bool ret = (0 == mdb_cursor_put(c.get(), &k, &v, MDB_CURRENT));
    return ret;
}

bool xmdb::cursor::put(const std::string& key, const std::string& value)
{
    MDB_val k, v;
    k.mv_data = const_cast<char*>(key.c_str());
    k.mv_size = key.length();
    v.mv_data = const_cast<char*>(value.c_str());
    v.mv_size = value.length();
    bool ret = (0 == mdb_cursor_put(c.get(), &k, &v, MDB_CURRENT));
    return ret;
}

bool xmdb::cursor::get_value(const std::string& key, int64_t& value)
{
    return get_value(key, &value, sizeof(value));
}

bool xmdb::cursor::get_value(const std::string& key, int32_t& value)
{
    return get_value(key, &value, sizeof(value));
}

bool xmdb::cursor::get_value(const std::string& key, void* data, int len,
        int* p_outlen)
{
    MDB_val k, v;
    k.mv_data = const_cast<char*>(key.c_str());
    k.mv_size = key.length();
    v.mv_data = 0; //bufv;
    v.mv_size = 0; //sizeof(bufv)-1;
    MDB_cursor_op op = MDB_SET;
    bool ret = (0 == mdb_cursor_get(this->c.get(), &k, &v, op));
    if (ret)
    {
        auto min = std::min(len, (int) v.mv_size);
        if (data)
        {
            memcpy(data, v.mv_data, min);
        }
        if (p_outlen)
        {
            *p_outlen = min;
        }
    }
    else
    {
        if (data)
        {
            memset(data, 0x00, len);
        }
        if (p_outlen)
        {
            *p_outlen = 0;
        }
    }
    return ret;
}

bool xmdb::cursor::get_value(const std::string& key, std::string& value)
{
    MDB_val k, v;
    k.mv_data = const_cast<char*>(key.c_str());
    k.mv_size = key.length();
    v.mv_data = 0; //bufv;
    v.mv_size = 0; //sizeof(bufv)-1;
    MDB_cursor_op op = MDB_SET;
    bool ret = (0 == mdb_cursor_get(this->c.get(), &k, &v, op));
    if (ret)
    {
        value.assign((char*) v.mv_data, v.mv_size);
    }
    else
    {
        value = "";
    }
    return ret;
}

bool xmdb::cursor::get_next(std::string& key, std::string& value)
{
    MDB_val k, v;

    k.mv_data = 0; //bufk;
    k.mv_size = 0; //sizeof(bufk)-1;
    v.mv_data = 0; //bufv;
    v.mv_size = 0; //sizeof(bufv)-1;
    MDB_cursor_op op = MDB_NEXT;
    bool ret = (0 == mdb_cursor_get(this->c.get(), &k, &v, op));
    if (ret)
    {
        key.clear();
        value.clear();
        key.assign((char*) k.mv_data, k.mv_size);
        value.assign((char*) v.mv_data, v.mv_size);
    }
    else
    {
        key = "";
        value = "";
    }
    return ret;
}


