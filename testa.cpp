//============================================================================
// Name        : testa.cpp
// Author      : 
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C++, Ansi-style
//============================================================================

#include <unistd.h>
#include <iostream>
using namespace std;

#include "xmdb.h"


int main(int argc, char **argv)
{
    auto p_xmdb = xmdb::get_instance(0);

    auto write_trans = p_xmdb->transaction_begin_rw();

    write_trans.put("abc1", "abcvalue1");
    write_trans.put("abc2", "abcvalue2");
    bool ret = write_trans.put("abc3", "abcvalue3");
    std::cout << "ret=" << ret << std::endl;
    write_trans.put_if_not_exist("abc33", "abcvalue3333");
    std::cout << "ret=" << ret << std::endl;

    write_trans.commit();

    while (0){
        {
            auto wtrans = p_xmdb->transaction_begin_rw();

        }
    }


    auto read_trans = p_xmdb->transaction_begin_rd();


    while (1) {
        std::string key, value;
        //p_xmdb->db().get(read_trans, "name", name );
        // read_trans.get("name", name);
        auto cursor = read_trans.cursor_open();

        while (cursor.get_next(key, value) ) {
            std::cout << key << ":" << value << std::endl;
        }

        cursor.get_value("fullname", value);
        std::cout << "fullname" << "::::::" << value << std::endl;
        bool ret = cursor.get_value("fullname", value);
        std::cout << "fullname" << "::::::" << value << " ret=" << ret << std::endl;
        cursor.get_next(key, value);
        std::cout << key << "::::::" << value << std::endl;

        ret = cursor.get_value("fullname00", value);
        std::cout << "fullname00" << "::::::" << value << " ret=" << ret << std::endl;

        std::cout << " *****************************************" << std::endl;
        sleep(1);
    }


    return 0;
}
