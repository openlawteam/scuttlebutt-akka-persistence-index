const FlumeviewLevel = require('flumeview-level');
const pull = require('pull-stream');

const takeRange = require('./util').takeRange;

const isPersistenceMessage = require('./util').isPersistenceMessage;

module.exports = (ssb, myKey, keysIndex) => {

    const indexVersion = 2;

    const index = ssb._flumeUse('akka-persistence-index',
        FlumeviewLevel(
            indexVersion,
            flumeMapFunction)
    )

    function flumeMapFunction(msg) {

        if (isPersistenceMessage(msg)) {
            const author = msg.value.author;
            const persistenceId = msg.value.content.persistenceId;

            const sequenceNr = msg.value.content.sequenceNr;

            const isEncrypted = msg.value.content.encrypted || false;

            // We only index the first item, as otherwise we would get repeats for live streams since old values
            // would be overrwritten to point to the latest message.
            if (sequenceNr == 1) {
                return [[author, isEncrypted, persistenceId], [persistenceId, isEncrypted, author]];
            }
            else {
                return [];
            }

            
        } else {
            return [];
        }

        
    }

    function persistenceIdsQuery(author, live) {

        return pull(index.read({
            gte: [author, null],
            lte: [author, undefined],
            live
        }), pull.map(value => {
            return value.value.value.content.persistenceId;
        }));
    }

    return {
        myCurrentPersistenceIds: (opts) => {
            opts = opts || {};

            const stream = persistenceIdsQuery(myKey, false);

            if (opts.start >= 0) {
                return takeRange(stream, opts.start, opts.end);
            } else {
                return stream;
            }

        },
        myCurrentPersistenceIdsAsync: (cb) => {
            pull(persistenceIdsQuery(myKey, false), pull.collect(cb));
        },
        myLivePersistenceIds: (opts) => {
            opts = opts || {};

            const source = persistenceIdsQuery(myKey, true);
            if (opts.start >= 0) {
                return takeRange(source, opts.start, opts.end);
            } else {
                return source;
            }
        },
        authorsForPersistenceId: (persistenceId, opts) => {
            opts = opts || {};

            const source = pull(
                index.read({
                    gte: [persistenceId, null, null],
                    lte: [persistenceId, undefined, undefined],
                    live: opts.live,
                    reverse: opts.reverse,
                    keys: true
                }),
                pull.asyncMap((result, cb) => {
                    const data = result.key;

                    const persistenceId = data[0];
                    const isEncrypted = data[1];
                    const author = data[2];

                    if (!isEncrypted) {
                        cb(null, {
                            data: data,
                            isEncrypted: false,
                            keys: []
                        });
                    } else {
                        // Get our keys for the entity for the given author (if any.)
                        keysIndex.getAllKeysFor(persistenceId, author).then(
                            keys => {
                                return {
                                    data: data,
                                    isEncrypted: true,
                                    keys: keys
                                }
                            }
                        ).asCallback(cb);
                    }

                }),
                // Filter out any persistenceIds that are private and we don't have the keys for.
                pull.filter(item => item.isEncrypted === false || item.keys.length > 0),
                pull.map(result => {
                    return result.data[2];
                }))

                if (opts.start >= 0) {
                    return takeRange(source, opts.start, opts.end);
                } else {
                    return source;
                }
        },
        persistenceIdsForAuthor: (authorId, opts) => {
            authorId = authorId || myKey;
            opts = opts || {};

            const source = pull(
                index.read({
                    gte: [authorId, null, null],
                    lte: [authorId, undefined, undefined],
                    live: opts.live,
                    keys: true
                }),
                pull.asyncMap((data, cb) => {
                    const isEncrypted = data.key[1] || false;

                    if (!isEncrypted) {
                        
                        cb(null, {
                            data: data.key,
                            isEncrypted: false,
                            keys: []
                        })
                    } else {
                        const persistenceId = data.key[2];
                        const authorId = data.key[0];

                        keysIndex.getAllKeysFor(persistenceId, authorId).then(
                            keys => {

                                return {
                                    data: data.key,
                                    isEncrypted: true,
                                    keys: keys
                                }
                            }

                        ).asCallback(cb);
                    }
                }),
                pull.filter(item => {
                    return !item.isEncrypted || (item.keys.length > 0)
                }),
                pull.map( item => {
                    const persistenceId = item.data[2];
                    return persistenceId;
                })
            );

            if (opts.start >= 0) {
                return takeRange(source, opts.start, opts.end);
            } else {
                return source;
            }

        },
        allAuthors: (opts) => {
            opts = opts || {};

            const source = pull(
                index.read({
                    gte: [null, null, null],
                    lte: [undefined, undefined, undefined],
                    live: opts.live,
                    keys: true
                }),
                pull.filter(item => {
                    return item.key[0].startsWith('@');
                }),
                pull.map(item => {
                    return item.key[0];
                }),
                pull.unique()
            );

            if (opts.start >= 0) {
                return takeRange(source, opts.start, opts.end);
            } else {
                return source;
            }

        }
    }
}

