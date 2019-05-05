const FlumeviewLevel = require('flumeview-level');
const pull = require('pull-stream');

const isPersistenceMessage = require('./util').isPersistenceMessage;

module.exports = (ssb, myKey) => {

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

            // We only index the first item, as otherwise we would get repeats for live streams since old values
            // would be overrwritten to point to the latest message.
            if (sequenceNr == 1) {
                return [[author, persistenceId], [persistenceId, author], [author]];
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
        myCurrentPersistenceIds: () => {
            return persistenceIdsQuery(myKey, false);
        },
        myCurrentPersistenceIdsAsync: (cb) => {
            pull(persistenceIdsQuery(myKey, false), pull.collect(cb));
        },
        myLivePersistenceIds: () => {
            return pull(persistenceIdsQuery(myKey, true))
        },
        authorsForPersistenceId: (persistenceId, opts) => {
            opts = opts || {};

            return pull(
                index.read({
                    gte: [persistenceId, null],
                    lte: [persistenceId, undefined],
                    live: opts.live
                }), 
                pull.map(item => {
                    const key = item.key;
                    return key[1];
                }))
        },
        persistenceIdsForAuthor: (authorId, opts) => {
            opts = opts || {};

            return pull(
                index.read({
                    gte: [authorId, null],
                    lte: [authorId, undefined],
                    live: opts.live
                }), pull.map( item => {
                    const persistenceId = item.key[1];
                    return persistenceId;
                })
            );
        },
        allAuthors: (opts) => {
            opts = opts || {};

            return pull(
                index.read({
                    gte: [null],
                    lte: [undefined],
                    live: opts.live,
                    keys: true
                }), pull.map(item => {
                    return item.key[0];
                }),
                pull.unique()
            );

        }
    }
}

