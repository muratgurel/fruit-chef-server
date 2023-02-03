// Customs errors
const errInternal: nkruntime.Error = {
    message: 'Internal error',
    code: nkruntime.Codes.INTERNAL
};

const errPermissionDenied: nkruntime.Error = {
    message: 'Permission denied',
    code: nkruntime.Codes.PERMISSION_DENIED
};

// Helper function to choose the reward for a rank in the tournament
function getReward(rewards: { [key: string]: any }, rank: number, score: number) {
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
function SortRecords(records: nkruntime.LeaderboardRecord[] | undefined) {
    // Sort by score, desc
    const sorted = records?.sort((a, b) => b.score - a.score);
    // Update its rank
    sorted?.forEach((record, index) => record.rank = index + 1);

    return sorted;
}

const InitModule: nkruntime.InitModule =
    function (ctx: nkruntime.Context, logger: nkruntime.Logger, nk: nkruntime.Nakama, initializer: nkruntime.Initializer) {
        // Register out RPCs
        initializer.registerRpc('server_create_live_event', serverCreateLiveEventRpc);
        initializer.registerRpc('get_bucketed_tournament', getBucketedTournamentRpc);
        initializer.registerTournamentEnd(distributeEventRewards);
        initializer.registerAfterAuthenticateDevice(initializeUser);
    }

// Hook to initialize a newly registered user
const initializeUser: nkruntime.AfterHookFunction<nkruntime.Session, nkruntime.AuthenticateDeviceRequest> = function (ctx: nkruntime.Context, logger: nkruntime.Logger, nk: nkruntime.Nakama, out: nkruntime.Session, data: nkruntime.AuthenticateDeviceRequest): nkruntime.Session {
    // Only initialise a player when they are registering for the first time
    if (out.created) {
        const changeset = {
            "coins": 100, // Add 100 coins to the user's wallet
        };

        try {
            nk.walletUpdate(ctx.userId, changeset, undefined, true);
        } catch (error: any) {
            logger.error('Error while initializing user: %s', error.message);
        }
    }

    return out;
};

// Our hook to distribute the event rewards when the event ends
const distributeEventRewards: nkruntime.TournamentEndFunction = function (ctx: nkruntime.Context, logger: nkruntime.Logger, nk: nkruntime.Nakama, tournament: nkruntime.Tournament, end: number, reset: number) {
    logger.debug("Distributing event rewards for %s", tournament.id);

    // Rewards are defined in a tournament's metadata during creation
    let rewards = tournament.metadata["rewards"];
    if (!rewards) {
        logger.debug("No rewards are defined for %s", tournament.id);
        return;
    }

    try {
        // We'll report execution time as a custom metric. Mark the start time.
        let startTime = Date.now();

        let notifications: nkruntime.NotificationRequest[] = [];
        let walletUpdates: nkruntime.WalletUpdate[] = []

        // Go over all records and assign rewards

        // Pagination cursor for listing records. We need it if 
        // there are more records than the limit passed
        let cursor: string | undefined = "";

        while (cursor != undefined) {
            // Pass in the reset timestamp to override expiry date so
            // we can iterate over the expired records to distribute rewards.
            // Otherwise, this call will return null as tournament has rolled over
            let results = nk.tournamentRecordsList(tournament.id, [], 100, cursor, reset);
            results.records?.forEach(function (r) {
                let reward = getReward(rewards, r.rank, r.score);
                if (reward) {
                    // This player has received a reward.
                    // Update their wallet and send them a notification
                    notifications.push({
                        code: 1, // This code is unique to identify the reward notification
                        content: {
                            // Fill out the details of the notification
                            // Client will fill the popup based on this information
                            "event_name": tournament.title,
                            "reward": reward,
                            "rank": r.rank
                        },
                        persistent: true, // We want offline players to see this notification as well when they come back online
                        subject: "event_reward",
                        userId: r.ownerId,
                    });

                    walletUpdates.push({
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
        nk.walletsUpdate(walletUpdates, true);
        nk.notificationsSend(notifications);

        logger.debug("%d players received rewards from the event %s", walletUpdates.length, tournament.id);

        // Calculate the execution time and report it
        // Date.now() measures in millisecond
        let endTime = Date.now();
        let executionTime = endTime - startTime;

        let name = "rewardDistributionElapsedTimeSec";
        let tags: { [key: string]: string } = {};

        nk.metricsTimerRecord(name, tags, executionTime);
    } catch (error: any) {
        logger.error('Error while distributing rewards: %s', error.message);
    }
}

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
const serverCreateLiveEventRpc: nkruntime.RpcFunction = function (ctx: nkruntime.Context, logger: nkruntime.Logger, nk: nkruntime.Nakama, payload: string): string | void {
    if (ctx.userId) {
        // Reject client-to-server calls
        throw errPermissionDenied;
    }

    try {
        // Parse the request payload
        let message = JSON.parse(payload);

        // Set up the bucketed tournament
        const id = message["id"];
        const authoritative = false; // We are okay letting clients do updates
        const sortOrder = nkruntime.SortOrder.DESCENDING;
        const operator = nkruntime.Operator.INCREMENTAL;
        const duration = message["duration"];
        const resetSchedule = null; // If null, no reset schedule. Event ends at endTime
        const metadata = { "rewards": message["rewards"] };
        const title = null;
        const description = null;
        const category = 1;
        const startTime = message["start_time"];
        const endTime = 0; // 0 will make sure filters in client work properly to fetch the active events
        const maxSize = null; // Max possible
        const maxNumScore = null; // Max possible
        const joinRequired = false;

        nk.tournamentCreate(id, authoritative, sortOrder, operator, duration, resetSchedule, metadata, title, description, category, startTime, endTime, maxSize, maxNumScore, joinRequired);
    } catch (error: any) {
        logger.error('Error while creating tournament: %s', error.message);
        throw errInternal;
    }

    return JSON.stringify({ "success": true });
}

// Define the bucketed tournament storage object
interface UserBucketStorageObject {
    resetTimeUnix: number,
    userIds: string[]
}

// Return the records for a bucketed tournament
// A sample request body:
// { 
//   "id": "live_event_2",
//   "limit": 32 
// }
// Response is same as tournamentRecordsList
const getBucketedTournamentRpc: nkruntime.RpcFunction = function (ctx: nkruntime.Context, logger: nkruntime.Logger, nk: nkruntime.Nakama, payload: string): string | void {
    if (!ctx.userId) {
        // Reject server-to-server call
        throw errPermissionDenied;
    }

    try {
        const bucketSize = 15;

        // Parse the request payload
        const message = JSON.parse(payload);
        const tournamentId = message["id"];
        const limit = message["limit"];

        const collection = 'buckets';
        const key = 'bucket';

        const objects = nk.storageRead([
            {
                collection,
                key,
                userId: ctx.userId
            }
        ]);

        // Fetch any existing bucket or create one if none exist
        let userBucket: UserBucketStorageObject = { resetTimeUnix: 0, userIds: [] };

        if (objects.length > 0) {
            userBucket = objects[0].value as UserBucketStorageObject;
        }

        // Fetch the tournament
        const tournaments = nk.tournamentsGetId([tournamentId]);

        // Tournament has reset or no current bucket exists for user
        if (userBucket.resetTimeUnix != tournaments[0].endActive || userBucket.userIds.length < bucketSize) {
            logger.debug(`getBucketedTournamentRpc new bucket for ${ctx.userId}`);

            // Clear the array in case it had some players from before
            userBucket.userIds = [];

            const users = nk.usersGetRandom(bucketSize);
            users.forEach(function (user: nkruntime.User) {
                userBucket.userIds.push(user.userId);
            });

            // Set the Reset and Bucket end times to be in sync
            userBucket.resetTimeUnix = tournaments[0].endActive;

            // Store generated bucket for the user
            nk.storageWrite([{
                collection,
                key,
                userId: ctx.userId,
                value: userBucket,
                permissionRead: 0,
                permissionWrite: 0
            }]);
        }

        // Add self to the list of tournament records to fetch
        userBucket.userIds.push(ctx.userId);

        // Get the tournament records
        const records = nk.tournamentRecordsList(tournamentId, userBucket.userIds, limit);

        // Owner records are not sorted
        records.ownerRecords = SortRecords(records.ownerRecords);

        return JSON.stringify(records);
    } catch (error: any) {
        logger.error('Error while getting local tournaments: %s', error.message);
        throw errInternal;
    }
}
