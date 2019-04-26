const FlumeviewLevel = require('flumeview-level');
const pull = require('pull-stream');

module.exports = (sbot, myKey) => {

    const version = 1;

    const index = sbot._flumeUse('entity-events-index', FlumeviewLevel(version, mapFunction));

    function mapFunction(message) {

        if (isPersistenceMessage(message)) {

            const author = message.value.author;
            const persistenceId = message.value.content.persistenceId;
            const sequenceNr = message.value.content.sequenceNr;
            
            // In the future, messages that are too big for one write will be written across several
            const part = 1;

            return [[author, persistenceId, sequenceNr, part]];
        } else {
            return [];
        }
    }

    function isPersistenceMessage(message) {
        const type = message.value.content.type;
        return type === "akka-persistence-message";
    }

    function eventsByPersistenceId(authorId, persistenceId, fromSequenceNumber, toSequenceNumber) {
        const source = index.read({
            keys: true,
            gte: [authorId, persistenceId, fromSequenceNumber, null],
            lte: [authorId, persistenceId, toSequenceNumber, undefined]
        });

        return pull(source, pull.map(msg => msg.value.value.content));
    }

    function highestSequenceNumber(authorId, persistenceId, cb) {

        const source = eventsByPersistenceId(authorId, persistenceId, 0, undefined);

        pull(source, pull.collect( (err, result) => {
            if (err) {
                cb(err);
            } else if (result.length === 0) {
                cb(null, 0);
            } else {
                const lastItem = result[result.length - 1];

                cb(null, lastItem.value.content.sequenceNr);
            }
        }));

    }

    return {
        eventsByPersistenceId,
        highestSequenceNumber
    }

}