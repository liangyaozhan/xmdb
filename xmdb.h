/*
 * database_center.h
 *
 *  Created on: Jul 8, 2017
 *      Author: lyzh
 */

#ifndef XMDB_H_
#define XMDB_H_

#include <iostream>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <memory>
#include "lmdb.h"


class xmdb
{
public:
    xmdb(const char *_path);
    virtual ~xmdb();

    // cursor for a trans
    class cursor
    {
    private:
        std::shared_ptr<MDB_cursor> c;
        cursor();
        cursor(MDB_cursor *_c):c(std::shared_ptr<MDB_cursor>(_c, [_c](void*f){
            mdb_cursor_close(_c);
        } )){}
        friend class xmdb;

    public:
        cursor(const cursor &o):c(o.c){}
        cursor &operator = (const cursor &o){this->c = o.c;return *this;}
        ~cursor(){}
    public:
        bool get_next(std::string &key, std::string &value);
        bool get_value(const std::string &key, std::string &value);
        bool get_value(const std::string &key, void *data, int len, int *p_outlen = 0);
        bool get_value(const std::string &key, int32_t &value);
        bool get_value(const std::string &key, int64_t &value);
        bool put(const std::string &key, const std::string &value);
        bool put(const std::string &key, const char *data, int len);
        bool put(const std::string &key, int32_t value );
    };

    // trans for a database
    class trans
    {
    private:
        trans() = delete;
        std::shared_ptr<MDB_txn> txn;
        MDB_dbi dbi;
        trans(MDB_dbi d, MDB_txn *t);
        friend class xmdb;

    public:
        trans(const trans &o):txn(o.txn), dbi(o.dbi){}
        trans &operator = (const trans &o);
        ~trans(){
        }

    public:
        cursor cursor_open();
        MDB_dbi db()const;

    public:
        bool put(const std::string &key, const std::string &value);
        bool put(const std::string &key, const char *data, int len);
        bool put(const std::string &key, int32_t value );
        bool put_if_not_exist(const std::string &key, const std::string &value);
        bool put_if_not_exist(const std::string &key, const char *data, int len);
        bool put_if_not_exist(const std::string &key, int32_t value );

        bool get_value(const std::string &key, std::string &value);
        bool get_value(const std::string &key, void *data, int len, int *p_outlen = 0);
        bool get_value(const std::string &key, int32_t &value);
        bool get_value(const std::string &key, int64_t &value);
        bool get_value(const std::string &key, uint16_t &value);

        bool commit();
        void abort();
        void rollback(){ abort();}
    };

    /* xmdb factory interface */
    static xmdb *get_instance( int index );
    static xmdb *get_instance( const std::string &dbname );
    trans transaction_begin_rw();
    trans transaction_begin_rd();
    MDB_dbi db(){ return this->dbi;}

private:
    const char *path;
	MDB_env *dbenv;
	MDB_dbi dbi;
	MDB_txn *txn;
	MDB_stat mst;

    void init_lmdb();

    xmdb() = delete;
    xmdb &operator = (const xmdb &o) = delete;
};



#endif /* FIAR_DATABASE_CENTER_H_ */
