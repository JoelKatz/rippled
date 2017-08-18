//------------------------------------------------------------------------------
/*
    This file is part of rippled: https://github.com/ripple/rippled
    Copyright (c) 2012, 2013 Ripple Labs Inc.

    Permission to use, copy, modify, and/or distribute this software for any
    purpose  with  or without fee is hereby granted, provided that the above
    copyright notice and this permission notice appear in all copies.

    THE  SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
    WITH  REGARD  TO  THIS  SOFTWARE  INCLUDING  ALL  IMPLIED  WARRANTIES  OF
    MERCHANTABILITY  AND  FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
    ANY  SPECIAL ,  DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
    WHATSOEVER  RESULTING  FROM  LOSS  OF USE, DATA OR PROFITS, WHETHER IN AN
    ACTION  OF  CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
    OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
//==============================================================================

#include <BeastConfig.h>
#include <test/jtx.h>
#include <ripple/beast/unit_test.h>

#include <ripple/nodestore/Database.h>
#include <test/nodestore/TestBase.h>
#include <ripple/app/main/NodeStoreScheduler.h>
#include <ripple/nodestore/Manager.h>
#include <ripple/beast/utility/temp_dir.h>
#include <ripple/core/JobQueue.h>
#include <chrono>

namespace ripple {
namespace NodeStore {

class Database_test : public TestBase
{
public:
    void run ()
    {
        using namespace test::jtx;
        Env env(*this);
        RootStoppable parent("TestRootStoppable");
        NodeStoreScheduler scheduler(parent);
        scheduler.setJobQueue(env.app().getJobQueue());

        beast::temp_dir node_db;
        Section nodeParams;
        nodeParams.set("path", node_db.path());
        nodeParams.set("type", "rocksdb");
        nodeParams.set("open_files", "2000");
        nodeParams.set("filter_bits", "12");
        nodeParams.set("cache_mb", "256");
        nodeParams.set("file_size_mb", "8");
        nodeParams.set("file_size_mult", "2");

        beast::xor_shift_engine rng(50);
        beast::Journal j;
        std::unique_ptr <Database> db =
            Manager::instance().make_Database(
                "test", scheduler, 2, parent, nodeParams, j);

        for (auto i = 0; i < 1000000; ++i)
        {
            std::cerr << "Write_load: " << db->getWriteLoad() << std::endl;
            while(db->getWriteLoad() >= 8000)
                std::this_thread::sleep_for(10ms);
            auto batch = createPredictableBatch(2000, rng());
            storeBatch(*db, batch);
        }
    }
};

BEAST_DEFINE_TESTSUITE(Database,NodeStore,ripple);

}
}