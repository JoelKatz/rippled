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

#ifndef RIPPLE_APP_MISC_FEETRACK_H_INCLUDED
#define RIPPLE_APP_MISC_FEETRACK_H_INCLUDED

#include <ripple/ledger/ReadView.h>
#include <ripple/shamap/SHAMap.h>
#include <ripple/protocol/STValidation.h>

namespace ripple {

/** Manager to track transaction clearance times by fee. */
class FeeLevelTrack
{
private:

    static constexpr int maxLedgerRange_ = 10;
    static constexpr std::chrono::seconds maxRangeValid_ = 180s;

    class FeeRange
    {
        public:

        // a range of fees that we aggregate for statistics
        std::uint64_t minFee_, maxFee_;
        NetClock::time_point lastUpdate_ = NetClock::time_point {};
        int ledgersX256_ = 0;
        int secondsX256_ = 0;
        int failX256_ = 0; // 0 = no failures, 256*100 = 100% failures
        int countX256_ = 0; // how many transactions we're seeing

        FeeRange (std::uint64_t minFee, std::uint64_t maxFee) :
            minFee_ (minFee), maxFee_ (maxFee)
        { ; }

        bool valid (NetClock::time_point now) const
        {
            return ((lastUpdate_ + maxRangeValid_) > now);
        }

        bool shouldCount (int validLedgers_)
        {
             if (validLedgers_ == 0)
                 return true;

             if (validLedgers_ >= maxLedgerRange_)
                 return true;

             if (ledgersX256_ == 0)
                 return false;

             return ((validLedgers_ * 192) >= (ledgersX256_ + 128));
        }

        void addFail (NetClock::time_point now, int validLedgers)
        {
            if (now != lastUpdate_)
            {
                if (! valid (now))
                {
                    ledgersX256_ = 0;
                    secondsX256_ = 0;
                    failX256_ = 100 * 256;
                    countX256_ = 0;
                    lastUpdate_ = now;
                    return;
                }

                while (lastUpdate_ < now)
                {
                    lastUpdate_ += 1s;
                    countX256_ = countX256_ * 255 / 256;
                }
            }
            if (shouldCount (validLedgers))
            {
                failX256_ = (failX256_ * 255 + 128) / 256 + 100;
                countX256_ += 256;
            }
        }

        void addSuccess (
            NetClock::time_point now,
            int ledgers, int seconds, int validLedgers_)
        {
            if (! valid (now))
            {
                ledgersX256_ = ledgers * 256;
                secondsX256_ = seconds * 256;
                countX256_ = 0;
                failX256_ = 0;

                lastUpdate_ = now;
            }
            else
            {
                while (lastUpdate_ < now)
                {
                    lastUpdate_ += 1s;
                    countX256_ = countX256_ * 255 / 256;
                }

                ledgersX256_ =
                    (ledgersX256_ * 255 + 128) / 256 + ledgers;
                secondsX256_ =
                    (secondsX256_ * 255 + 128) / 256 + seconds;

                if (shouldCount (validLedgers_))
                {
                    countX256_ += 256;
                    failX256_ = failX256_ * 255 / 256;
                }
            }
        }
    };

    class FTTx
    {
        public:

        // a transaction that is not yet fully validated
        std::uint64_t fee_;
        NetClock::time_point timeSeen_;
        LedgerIndex ledgerSeen_;
        int validLedgers_;

        FTTx (
           std::uint64_t fee,
           NetClock::time_point timeSeen,
           LedgerIndex ledgerSeen,
           int validLedgers) :
               fee_ (fee),
               timeSeen_ (timeSeen),
               ledgerSeen_ (ledgerSeen),
               validLedgers_ (validLedgers)
        { ; }
    };

public:

    FeeLevelTrack ()
    {
        ranges_.emplace_back(10, 10);
        ranges_.emplace_back(11, 19);
        ranges_.emplace_back(20, 49);
        ranges_.emplace_back(50, 99);
        ranges_.emplace_back(100, 199);
        ranges_.emplace_back(200, 499);
        ranges_.emplace_back(500, 999);
        ranges_.emplace_back(1000, 1999);
        ranges_.emplace_back(2000, 4999);
        ranges_.emplace_back(5000, 9999);
        ranges_.emplace_back(10000, 19999);
        ranges_.emplace_back(20000, 49999);
        ranges_.emplace_back(50000, 99999);
        ranges_.emplace_back(100000, 999999);
        ranges_.emplace_back(1000000, 9999999);
    }

    // called when a new transaction is received from a peer
    // and it either gets into our open ledger (tes or tec)
    // or is queued normally
    void trackTransaction (STTx const& tx,
        ReadView const& validatedLedger,
        NetClock::time_point now)
    {
        if (tx.isFieldPresent(sfPreviousTxnID) ||
            tx.isFieldPresent(sfAccountTxnID) ||
            tx.isFieldPresent(sfSigners))
        {
            // These fields can affect transaction clearance times
            // or fee levels.
            return;
        }

        // This could result in a delay
        auto const dstAcct = validatedLedger.read (
            keylet::account (tx.getAccountID (sfAccount)));
        if (! dstAcct ||
            (dstAcct->getFieldU32 (sfSequence) !=
               tx.getFieldU32 (sfSequence)))
        {
            // This transaction can be held due to prior transactions
            // not clearing.
            return;
        }

        // Okay, transaction seems "pure".

        int validLedgers = 0;
        if (tx.isFieldPresent (sfLastLedgerSequence))
        {
            auto const lls = tx.getFieldU32 (sfLastLedgerSequence);
            if (lls <= validatedLedger.seq())
                return;
            validLedgers = lls - validatedLedger.seq();
        }

        {
            std::lock_guard <std::mutex> lock (mutex_);

            txns_.emplace (std::piecewise_construct,
                std::forward_as_tuple (tx.getTransactionID()),
                std::forward_as_tuple (tx.getFieldAmount(sfFee).xrp().drops(),
                    now, validatedLedger.seq(), validLedgers));
        }
    }

    // called when a new ledger is fully-validated
    void validatedLedger (ReadView const& validatedLedger,
        NetClock::time_point now)
    {
        auto const seq = validatedLedger.seq();
        auto const expireSeq = (seq < maxLedgerRange_) ? 0
            : (seq - maxLedgerRange_);

        std::lock_guard <std::mutex> lock (mutex_);

        for (auto const& tx : validatedLedger.txs)
        {
            auto txnEntry = txns_.find (tx.first->getTransactionID());
            if (txnEntry != txns_.end())
            {
                auto& tx = txnEntry->second;

                if ((seq >= tx.ledgerSeen_) &&
                    (now >= tx.timeSeen_))
                {
                    auto range = findFeeRange (tx.fee_);
                    if (range)
                        range->addSuccess (now,
                            seq - tx.ledgerSeen_,
                            std::chrono::duration_cast
                                <std::chrono::seconds>
                                    (now - tx.timeSeen_).count(),
                                        tx.validLedgers_);
                }
                txns_.erase (txnEntry);
            }
        }

        auto it = txns_.begin();
        while (it != txns_.end())
        {
            if (it->second.ledgerSeen_ < expireSeq)
            {
                auto range = findFeeRange (it->second.fee_);
                if (range)
                    range->addFail (now, it->second.validLedgers_);
                it = txns_.erase (it);
            }
            else
                ++it;
        }
    }

    // called if we have a ledger jump or some other odd issue
    void clear ()
    {
        std::lock_guard <std::mutex> lock (mutex_);
        txns_.clear();
    }

    Json::Value getJson(NetClock::time_point now) const
    {
        Json::Value ret = Json::arrayValue;
        {
            std::lock_guard <std::mutex> lock (mutex_);

            for (auto& band : ranges_)
            {
                if (band.valid (now))
                {
                    Json::Value v = Json::objectValue;
                    v["FeeMin"] = static_cast<int> (band.minFee_);
                    v["FeeMax"] = static_cast<int> (band.maxFee_);
                    v["Ledgers"] = static_cast<int> ((band.ledgersX256_ + 128) / 256);
                    v["Seconds"] = static_cast<int> ((band.secondsX256_ + 128) / 256);
                    if (band.countX256_ > 0)
                        v["Count"] = static_cast<int> ((band.countX256_ + 128) / 256);
                    if (band.failX256_ > 0)
                        v["Fail"] = (band.failX256_ + 128) / 256;
                    ret.append (std::move (v));
                }
            }
        }
        return ret;
    }

private:

    std::mutex mutable mutex_;

    hash_map <uint256, FTTx> txns_;
    std::vector<FeeRange> ranges_;

    FeeRange* findFeeRange (std::uint64_t fee)
    {
        for (auto &range : ranges_)
        {
            // FIXME: Should use binary search
            if ((fee >= range.minFee_) && (fee <= range.maxFee_))
                return &range;
        }
        return nullptr;
    }
};


} // ripple

#endif
