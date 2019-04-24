const PersistenceIdsIndex = require('./persistenceIdsIndex');
const EntityEventsIndex = require('./entityEventsIndex');

const AccessIndex = require('./auth/index');

exports.name = 'akka-persistence-index'

exports.version = require('./package.json').version

exports.manifest = {
    currentPersistenceIds: 'source',
    currentPersistenceIdsAsync: 'async',
    livePersistenceIds: 'source',

    eventsByPersistenceId: 'source',
    highestSequenceNumber: 'async',
    persistEvent: 'async'
}

const indexVersion = 1;

exports.init = (ssb, config) => {

    const persistenceIdsIndex = PersistenceIdsIndex(ssb, '@' + config.keys.public);
    const accessIndex = AccessIndex(ssb, config);
    const entityEventsIndex = EntityEventsIndex(ssb, '@' + config.keys.public);

    /**
     * The decrypted stream of events persisted for the given entity ID, up to the last sequence number visible
     * to the user.
     * 
     * @param {*} authorId the author of the events (or ourselves if null.)
     * @param {*} persistenceId the peristence ID of the entity
     * @param {*} fromSequenceNumber the start sequence number to stream from
     * @param {*} toSequenceNumber the maximum sequence number to stream up to (stream ends early if the last item has a smaller
     * sequence number than this.)
     */
    function eventsByPersistenceId(authorId, persistenceId, fromSequenceNumber, toSequenceNumber) {
        const encryptedSource = entityEventsIndex.eventsByPersistenceId(authorId, persistenceId, fromSequenceNumber, toSequenceNumber);

        // TODO: decryption would go here as a stream map.

        return encryptedSource;
    }

    function decrypt(message) {

    }

    /**
     * Returns the highest sequence number of an entity by sequence number for
     * an entity someone has authored
     * 
     * @param {*} authorId the author of the events (or ourselves if null.)
     * @param {*} persistenceId the persistence ID of the entity
     */
    function highestSequenceNumber(authorId, persistenceId) {
        return entityEventsIndex.highestSequenceNumber(authorId, persistenceId);
    }

    /**
     * Persists the message to the log.
     * 
     * If the persisted message is one which adds a user to the access list (AddAccess),
     * then as well as persisting this message to the log, a private message is sent to
     * the user with the scuttlebutt key in the AddAccess payload with the list of decryption
     * keys and the sequence number they apply to.
     * 
     * The private message is sent using ssb-private (https://github.com/ssbc/ssb-private)
     * 
     * Note: if this payload exceeds the maximum message length (if the key has changed frequently),
     * we will have to send it as several private messages instead. Example payload:
     * 
     * {
     *    type: "akka-persistence-keys",
     *    content: {
     *    persistenceId: <persistence ID of the entity>
     *    keys: [
     *     {
     *       startSequenceNumber: 0,
     *       key: "..."
     *     },
     *     {
     *       startSequenceNumber: 10,
     *       key: "..."
     *     }
     *    ]
     *   }
     *
     * }
     * 
     * If the message removes someone from the access list, then the key is changed and all the remaining
     * people with access are private messaged the new key with the same schema as above, but with only
     * one item (the new key) in the keys field.
     * 
     * @param {*} persistedMessage expected to follow the schema of a PersistentRepr in akka
     * (https://doc.akka.io/japi/akka/current/akka/persistence/PersistentRepr.html)
     */
    function persistEvent(persistenceId, persistedMessage) {

    }

    return {
        eventsByPersistenceId: eventsByPersistenceId,
        highestSequenceNumber: highestSequenceNumber,
        persistEvent: persistEvent,

        currentPersistenceIds: () => {
            return persistenceIdsIndex.currentPersistenceIds();
        },
        currentPersistenceIdsAsync: (cb) => {
            return persistenceIdsIndex.currentPersistenceIdsAsync(cb);
        },
        livePersistenceIds: () => {
            return persistenceIdsIndex.livePersistenceIds();
        }
    }

}




