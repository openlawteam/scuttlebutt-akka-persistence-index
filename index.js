const pull = require('pull-stream');
const FlumeReduce = require('flumeview-reduce');

exports.name = 'akka-persistence-index'
exports.version = require('./package.json').version

exports.manifest = {
    currentPersistenceIds: 'source',
    currentPersistenceIdsAsync: 'async',
    livePersistenceIds: 'source'
}

const indexVersion = 1;

exports.init = (ssb, config) => {
    const view = ssb._flumeUse('akka-persistence-index',
        FlumeReduce(
        indexVersion,
        flumeReduceFunction,
        flumeMapFunction)
    )

    function flumeReduceFunction(index, persistenceId) {

        if (!index) index = []

        if (!index.includes(persistenceId)) {
            index.push(persistenceId)
        }

        return index;
    }

    function flumeMapFunction(item) {
        // If this is a akka persistence type message, we return it - if there isn't, the reducer isn't ran for 'undefined'
        // values
        return item.value.content['persistenceId'];
    }

    /**
     * Returns a list of strings (the current persistenceIds) of which the user can see some or all of the 
     * updates for. If the user had a key for some of the updates, but access is revoked, they will receive that
     * ID as part of this list but not be able to query for further updates than they had the key for.
     */
    function currentPersistenceIdsAsync() {

    }

    /**
     * The decrypted stream of events persisted for the given entity ID, up to the last sequence number visible
     * to the user.
     * 
     * @param {*} persistenceId the peristence ID of the entity
     * @param {*} fromSequenceNumber the start sequence number to stream from
     * @param {*} toSequenceNumber the maximum sequence number to stream up to (stream ends early if the last item has a smaller
     * sequence number than this.)
     */
    function eventsByPersistenceId(persistenceId, fromSequenceNumber, toSequenceNumber) {

    }

    /**
     * Returns the highest sequence number of an entity by sequence number that
     * is visible to the user (they may have been removed from the access list at
     * some point.)
     * 
     * @param {*} persistenceId the persistence ID of the entity
     */
    function highestSequenceNumber(persistenceId) {

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
        currentPersistenceIds: () => {
            return pull(view.stream({live: false}), pull.flatten())
        },
        currentPersistenceIdsAsync: (cb) => {
            view.get(cb)
        },
        livePersistenceIds: () => {
            return pull(view.stream({live: true}), pull.flatten(), pull.unique(), pull.filter(item => !item.sync))
        }
    }

}




