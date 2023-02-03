"use strict";
// Customs errors
var errInternal = {
    message: 'Internal error',
    code: 13 /* nkruntime.Codes.INTERNAL */
};
var errPermissionDenied = {
    message: 'Permission denied',
    code: 7 /* nkruntime.Codes.PERMISSION_DENIED */
};
// Helper function to choose the reward for a rank in the tournament
function getReward(rewards, rank, score) {
    if (score == 0) {
        // We do not give rewards to players who haven't scored anything.
        return null;
    }
    // The whole number in the tournament metadata is represented
    // as float64 in go runtime so we cannot pass it directly.
    // We have to round it so we can ensure it's int64
    switch (rank) {
        case 1:
            return { "coins": Math.round(rewards["first"]["coins"]) };
        case 2:
            return { "coins": Math.round(rewards["second"]["coins"]) };
        case 3:
            return { "coins": Math.round(rewards["third"]["coins"]) };
        default:
            return { "coins": Math.round(rewards["rest"]["coins"]) };
    }
}
// Helper function to sort leaderboard records by descending scores
function SortRecords(records) {
    // Sort by score, desc
    var sorted = records === null || records === void 0 ? void 0 : records.sort(function (a, b) { return b.score - a.score; });
    // Update its rank
    sorted === null || sorted === void 0 ? void 0 : sorted.forEach(function (record, index) { return record.rank = index + 1; });
    return sorted;
}
var InitModule = function (ctx, logger, nk, initializer) {
    // Register out RPCs
    initializer.registerRpc('server_create_live_event', serverCreateLiveEventRpc);
    initializer.registerRpc('get_bucketed_tournament', getBucketedTournamentRpc);
    initializer.registerTournamentEnd(distributeEventRewards);
    initializer.registerAfterAuthenticateDevice(initializeUser);
};
// Hook to initialize a newly registered user
var initializeUser = function (ctx, logger, nk, out, data) {
    // Only initialise a player when they are registering for the first time
    if (out.created) {
        var changeset = {
            "coins": 100, // Add 100 coins to the user's wallet
        };
        try {
            nk.walletUpdate(ctx.userId, changeset, undefined, true);
        }
        catch (error) {
            logger.error('Error while initializing user: %s', error.message);
        }
    }
    return out;
};
// Our hook to distribute the event rewards when the event ends
var distributeEventRewards = function (ctx, logger, nk, tournament, end, reset) {
    var _a;
    logger.debug("Distributing event rewards for %s", tournament.id);
    // Rewards are defined in a tournament's metadata during creation
    var rewards = tournament.metadata["rewards"];
    if (!rewards) {
        logger.debug("No rewards are defined for %s", tournament.id);
        return;
    }
    try {
        // We'll report execution time as a custom metric. Mark the start time.
        var startTime = Date.now();
        var notifications_1 = [];
        var walletUpdates_1 = [];
        // Go over all records and assign rewards
        // Pagination cursor for listing records. We need it if 
        // there are more records than the limit passed
        var cursor = "";
        while (cursor != undefined) {
            // Pass in the reset timestamp to override expiry date so
            // we can iterate over the expired records to distribute rewards.
            // Otherwise, this call will return null as tournament has rolled over
            var results = nk.tournamentRecordsList(tournament.id, [], 100, cursor, reset);
            (_a = results.records) === null || _a === void 0 ? void 0 : _a.forEach(function (r) {
                var reward = getReward(rewards, r.rank, r.score);
                if (reward) {
                    // This player has received a reward.
                    // Update their wallet and send them a notification
                    notifications_1.push({
                        code: 1,
                        content: {
                            // Fill out the details of the notification
                            // Client will fill the popup based on this information
                            "event_name": tournament.title,
                            "reward": reward,
                            "rank": r.rank
                        },
                        persistent: true,
                        subject: "event_reward",
                        userId: r.ownerId,
                    });
                    walletUpdates_1.push({
                        userId: r.ownerId,
                        changeset: reward,
                    });
                }
            });
            // Set the pagination cursor to the next cursor returned by the call
            // If no more pages left, it will be null and loop will exit
            cursor = results.nextCursor;
        }
        // Submit the changes
        nk.walletsUpdate(walletUpdates_1, true);
        nk.notificationsSend(notifications_1);
        logger.debug("%d players received rewards from the event %s", walletUpdates_1.length, tournament.id);
        // Calculate the execution time and report it
        // Date.now() measures in millisecond
        var endTime = Date.now();
        var executionTime = endTime - startTime;
        var name_1 = "rewardDistributionElapsedTimeSec";
        var tags = {};
        nk.metricsTimerRecord(name_1, tags, executionTime);
    }
    catch (error) {
        logger.error('Error while distributing rewards: %s', error.message);
    }
};
// A server-to-server RPC to create/schedule a live event
// A sample request body:
// {
//   "id": "live_event_1",
//   "start_time": 0,
//   "duration": 259200,
//   "rewards": {
//     "first": { "coins": 250 },
//     "second": { "coins": 200 },
//     "third" : { "coins": 150 },
//     "rest": { "coins": 50 }
//   }
// }
var serverCreateLiveEventRpc = function (ctx, logger, nk, payload) {
    if (ctx.userId) {
        // Reject client-to-server calls
        throw errPermissionDenied;
    }
    try {
        // Parse the request payload
        var message = JSON.parse(payload);
        // Set up the bucketed tournament
        var id = message["id"];
        var authoritative = false; // We are okay letting clients do updates
        var sortOrder = "desc" /* nkruntime.SortOrder.DESCENDING */;
        var operator = "incr" /* nkruntime.Operator.INCREMENTAL */;
        var duration = message["duration"];
        var resetSchedule = null; // If null, no reset schedule. Event ends at endTime
        var metadata = { "rewards": message["rewards"] };
        var title = null;
        var description = null;
        var category = 1;
        var startTime = message["start_time"];
        var endTime = 0; // 0 will make sure filters in client work properly to fetch the active events
        var maxSize = null; // Max possible
        var maxNumScore = null; // Max possible
        var joinRequired = false;
        nk.tournamentCreate(id, authoritative, sortOrder, operator, duration, resetSchedule, metadata, title, description, category, startTime, endTime, maxSize, maxNumScore, joinRequired);
    }
    catch (error) {
        logger.error('Error while creating tournament: %s', error.message);
        throw errInternal;
    }
    return JSON.stringify({ "success": true });
};
// Return the records for a bucketed tournament
// A sample request body:
// { 
//   "id": "live_event_2",
//   "limit": 32 
// }
// Response is same as tournamentRecordsList
var getBucketedTournamentRpc = function (ctx, logger, nk, payload) {
    if (!ctx.userId) {
        // Reject server-to-server call
        throw errPermissionDenied;
    }
    try {
        var bucketSize = 15;
        // Parse the request payload
        var message = JSON.parse(payload);
        var tournamentId = message["id"];
        var limit = message["limit"];
        var collection = 'buckets';
        var key = 'bucket';
        var objects = nk.storageRead([
            {
                collection: collection,
                key: key,
                userId: ctx.userId
            }
        ]);
        // Fetch any existing bucket or create one if none exist
        var userBucket_1 = { resetTimeUnix: 0, userIds: [] };
        if (objects.length > 0) {
            userBucket_1 = objects[0].value;
        }
        // Fetch the tournament
        var tournaments = nk.tournamentsGetId([tournamentId]);
        // Tournament has reset or no current bucket exists for user
        if (userBucket_1.resetTimeUnix != tournaments[0].endActive || userBucket_1.userIds.length < bucketSize) {
            logger.debug("getBucketedTournamentRpc new bucket for ".concat(ctx.userId));
            // Clear the array in case it had some players from before
            userBucket_1.userIds = [];
            var users = nk.usersGetRandom(bucketSize);
            users.forEach(function (user) {
                userBucket_1.userIds.push(user.userId);
            });
            // Set the Reset and Bucket end times to be in sync
            userBucket_1.resetTimeUnix = tournaments[0].endActive;
            // Store generated bucket for the user
            nk.storageWrite([{
                    collection: collection,
                    key: key,
                    userId: ctx.userId,
                    value: userBucket_1,
                    permissionRead: 0,
                    permissionWrite: 0
                }]);
        }
        // Add self to the list of tournament records to fetch
        userBucket_1.userIds.push(ctx.userId);
        // Get the tournament records
        var records = nk.tournamentRecordsList(tournamentId, userBucket_1.userIds, limit);
        // Owner records are not sorted
        records.ownerRecords = SortRecords(records.ownerRecords);
        return JSON.stringify(records);
    }
    catch (error) {
        logger.error('Error while getting local tournaments: %s', error.message);
        throw errInternal;
    }
};
