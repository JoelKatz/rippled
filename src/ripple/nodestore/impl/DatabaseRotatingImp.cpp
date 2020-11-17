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

#include <ripple/app/ledger/Ledger.h>
#include <ripple/nodestore/impl/DatabaseRotatingImp.h>
#include <ripple/protocol/HashPrefix.h>

namespace ripple {
namespace NodeStore {

DatabaseRotatingImp::DatabaseRotatingImp(
    std::string const& name,
    Scheduler& scheduler,
    int readThreads,
    Stoppable& parent,
    std::shared_ptr<Backend> writableBackend,
    std::shared_ptr<Backend> archiveBackend,
    Section const& config,
    beast::Journal j)
    : DatabaseRotating(name, parent, scheduler, readThreads, config, j)
    , pCache_(std::make_shared<TaggedCache<uint256, NodeObject>>(
          name,
          cacheTargetSize,
          cacheTargetAge,
          stopwatch(),
          j))
    , nCache_(std::make_shared<KeyCache<uint256>>(
          name,
          stopwatch(),
          cacheTargetSize,
          cacheTargetAge))
    , writableBackend_(std::move(writableBackend))
    , archiveBackend_(std::move(archiveBackend))
{
    if (writableBackend_)
        fdRequired_ += writableBackend_->fdRequired();
    if (archiveBackend_)
        fdRequired_ += archiveBackend_->fdRequired();
    setParent(parent);
}

void
DatabaseRotatingImp::rotateWithLock(
    std::function<std::unique_ptr<NodeStore::Backend>(
        std::string const& writableBackendName)> const& f)
{
    std::lock_guard lock(mutex_);

    auto newBackend = f(writableBackend_->getName());
    archiveBackend_->setDeletePath();
    archiveBackend_ = std::move(writableBackend_);
    writableBackend_ = std::move(newBackend);
}

std::string
DatabaseRotatingImp::getName() const
{
    std::lock_guard lock(mutex_);
    return writableBackend_->getName();
}

std::int32_t
DatabaseRotatingImp::getWriteLoad() const
{
    std::lock_guard lock(mutex_);
    return writableBackend_->getWriteLoad();
}

void
DatabaseRotatingImp::import(Database& source)
{
    auto const backend = [&] {
        std::lock_guard lock(mutex_);
        return writableBackend_;
    }();

    importInternal(*backend, source);
}

bool
DatabaseRotatingImp::storeLedger(std::shared_ptr<Ledger const> const& srcLedger)
{
    auto const backend = [&] {
        std::lock_guard lock(mutex_);
        return writableBackend_;
    }();

    return Database::storeLedger(*srcLedger, backend, pCache_, nCache_);
}

void
DatabaseRotatingImp::store(
    NodeObjectType type,
    Blob&& data,
    uint256 const& hash,
    std::uint32_t)
{
    auto nObj = NodeObject::createObject(type, std::move(data), hash);
    pCache_->canonicalize_replace_cache(hash, nObj);

    auto const backend = [&] {
        std::lock_guard lock(mutex_);
        return writableBackend_;
    }();
    backend->store(nObj);

    nCache_->erase(hash);
    storeStats(1, nObj->getData().size());
}

bool
DatabaseRotatingImp::asyncFetch(
    uint256 const& hash,
    std::uint32_t ledgerSeq,
    std::shared_ptr<NodeObject>& nodeObject,
    std::function<void(std::shared_ptr<NodeObject>&)>&& callback)
{
    // See if the object is in cache
    nodeObject = pCache_->fetch(hash);
    if (nodeObject || nCache_->touch_if_exists(hash))
        return true;

    // Otherwise maybe post a read
    return Database::asyncFetch(
        hash, ledgerSeq, nodeObject, std::move(callback));
}

void
DatabaseRotatingImp::tune(int size, std::chrono::seconds age)
{
    pCache_->setTargetSize(size);
    pCache_->setTargetAge(age);
    nCache_->setTargetSize(size);
    nCache_->setTargetAge(age);
}

void
DatabaseRotatingImp::sweep()
{
    pCache_->sweep();
    nCache_->sweep();
}

std::shared_ptr<NodeObject>
DatabaseRotatingImp::fetchNodeObject(
    uint256 const& hash,
    std::uint32_t,
    FetchReport& fetchReport)
{
    auto fetch = [&](std::shared_ptr<Backend> const& backend) {
        Status status;
        std::shared_ptr<NodeObject> nodeObject;
        try
        {
            status = backend->fetch(hash.data(), &nodeObject);
        }
        catch (std::exception const& e)
        {
            JLOG(j_.fatal()) << "Exception, " << e.what();
            Rethrow();
        }

        switch (status)
        {
            case ok:
                ++fetchHitCount_;
                if (nodeObject)
                    fetchSz_ += nodeObject->getData().size();
                break;
            case notFound:
                break;
            case dataCorrupt:
                JLOG(j_.fatal()) << "Corrupt NodeObject #" << hash;
                break;
            default:
                JLOG(j_.warn()) << "Unknown status=" << status;
                break;
        }

        return nodeObject;
    };

    // See if the node object exists in the cache
    auto nodeObject{pCache_->fetch(hash)};
    if (!nodeObject && !nCache_->touch_if_exists(hash))
    {
        auto [writable, archive] = [&] {
            std::lock_guard lock(mutex_);
            return std::make_pair(writableBackend_, archiveBackend_);
        }();

        fetchReport.wentToDisk = true;

        // Try to fetch from the writable backend
        nodeObject = fetch(writable);
        if (!nodeObject)
        {
            // Otherwise try to fetch from the archive backend
            nodeObject = fetch(archive);
            if (nodeObject)
            {
                {
                    // Refresh the writable backend pointer
                    std::lock_guard lock(mutex_);
                    writable = writableBackend_;
                }

                // Update writable backend with data from the archive backend
                writable->store(nodeObject);
                nCache_->erase(hash);
            }
        }

        if (!nodeObject)
        {
            // Just in case a write occurred
            nodeObject = pCache_->fetch(hash);
            if (!nodeObject)
                // We give up
                nCache_->insert(hash);
        }
        else
        {
            fetchReport.wasFound = true;

            // Ensure all threads get the same object
            pCache_->canonicalize_replace_client(hash, nodeObject);

            // Since this was a 'hard' fetch, we will log it
            JLOG(j_.trace()) << "HOS: " << hash << " fetch: in shard db";
        }
    }

    return nodeObject;
}

void
DatabaseRotatingImp::for_each(
    std::function<void(std::shared_ptr<NodeObject>)> f)
{
    auto [writable, archive] = [&] {
        std::lock_guard lock(mutex_);
        return std::make_pair(writableBackend_, archiveBackend_);
    }();

    // Iterate the writable backend
    writable->for_each(f);

    // Iterate the archive backend
    archive->for_each(f);
}

}  // namespace NodeStore
}  // namespace ripple
