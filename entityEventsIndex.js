const FlumeviewLevel = require('flumeview-level');
const pull = require('pull-stream');

const window = require('pull-window');

const isPersistenceMessage = require('./util').isPersistenceMessage;

module.exports = (sbot, myKey) => {

    const version = 2;

    const index = sbot._flumeUse('entity-events-index', FlumeviewLevel(version, mapFunction));

    function mapFunction(message) {

        if (isPersistenceMessage(message)) {

            const author = message.value.author;
            const persistenceId = message.value.content.persistenceId;
            const sequenceNr = message.value.content.sequenceNr;

            const part = message.value.content.part || 1;

            return [[author, persistenceId, sequenceNr, part]];
        } else {
            return [];
        }
    }

    function eventsByPersistenceId(authorId, persistenceId, fromSequenceNumber, toSequenceNumber, live) {
        
        const source = index.read({
            keys: true,
            gte: [authorId, persistenceId, fromSequenceNumber, null],
            lte: [authorId, persistenceId, toSequenceNumber, undefined],
            live: live,
            sync: false
        });

        return pull(source, pull.map(msg => {
            let content = msg.value.value.content

            content['scuttlebuttSequence'] = msg.value.value.sequence;

            return content
        }), reAssemblePartsThrough()
        );
    }

    function reAssemblePartsThrough() {

        let windowing = false;

        return window(function (_, cb) {

            if (windowing) return;
            windowing = true;

            let parts = [];

            return function (end, data) {

                if (end && parts.length > 0) {
                    const lastPart = parts[parts.length - 1];

                    // If we haven't replicated the rest of the parts for the message,
                    // we act as though we haven't got any of it as we can't do anything
                    // with just part of
                    if (lastPart.part === lastPart.of) {
                        return cb(null, assembleParts(parts));
                    } else {
                        return cb(true, null);
                    }
                }
                else if (end) return cb(null, data);
                else if (!data.part) {
                    cb(null, data);
                    windowing = false;
                }
                else if (data.part === data.of) {
                    windowing = false;
                    parts.push(data);
                    cb(null, assembleParts(parts))
                }
                else {
                    parts.push(data);
                }
            }
        }, function (start, data) {
            return data;
        });
    }

    function assembleParts(parts) {

        const payloads = parts.map(part => part.payload);

        const fullPayload = payloads.join('');

        const full = parts[parts.length - 1];

        if (full.encrypted) {
            // Encrypted payloads are base64 strings until they're decrypted later in the
            // pipeline
            full.payload = fullPayload;
        } else {
            // Make it into an object again now that the string is joined up.
            full.payload = JSON.parse(fullPayload);
        }

        return full;
    }

    function highestSequenceNumber(authorId, persistenceId, cb) {

        const source = eventsByPersistenceId(authorId, persistenceId, 0, undefined);

        pull(source, pull.collect((err, result) => {

            if (err) {
                cb(err);
            } else if (result.length === 0) {
                cb(null, 0);
            } else {
                const lastItem = result[result.length - 1];

                cb(null, lastItem.sequenceNr);
            }
        }));
    }

    function allEventsForAuthor(authorId, startSequenceNr) {
        authorId = authorId || myKey;

        const source = sbot.query.read({
            query: [
              {$filter: {
                value: {
                    sequence: {$gte: startSequenceNr},
                    author: authorId,
                    content: { 
                        type: 'akka-persistence-message'
                    }
                }
              }}
            ]
          });

        const isMidPart = ((item) => {
            const isMid = item.value.content.part && (item.value.content.part < item.value.content.of);
            return isMid; 
        });

        return pull(source, 
            pull.filter(item => !isMidPart(item)),
            pull.asyncMap((item, cb) => {

                if (item.value.content.part) {
                    // Emit the full item
                    const sequenceNr = item.value.content.sequenceNr;

                    pull(eventsByPersistenceId(authorId, item.value.content.persistenceId, sequenceNr, sequenceNr + 1), 
                        pull.collect((err, results) => {
                            // Note: 'scuttlebuttSequence' is already included by the eventsByPersistenceId source.
                            // It is the scuttlebutt sequence number of the last 'part' for the persisted entity event
                            cb(err, results[0])
                        })
                    )
                } else {
                    const result = item.value.content;
                    result['scuttlebuttSequence'] = item.value.sequence;
                    cb(null, result);
                }

        }));
    }

    return {
        allEventsForAuthor,
        eventsByPersistenceId,
        highestSequenceNumber
    }

}
