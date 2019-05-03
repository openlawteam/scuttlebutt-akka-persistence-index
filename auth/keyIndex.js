const FlumeViewReduce = require('flumeview-reduce');
const R = require('ramda');
const constants = require('./constants');
const promisify = require('bluebird').promisify;

/**
 * An index of the private entities that we have been given keys to by someone or
 * by ourselves.
 */
module.exports = (sbot, myKey) => {

    const version = 1;

    const index = sbot._flumeUse('akka-persistence-key-index', FlumeViewReduce(version, reduce, map));

    function map(message) {

        const messageType = message.value.content.type;
        const author = message.value.author;

        if (messageType === constants.updateKeyMessageType) {

            const persistenceId = message.value.content.persistenceId;
            const keys = message.value.content.keys;

            return {
                persistenceId: persistenceId,
                keys,
                author
            }
        }

    }

    function reduce(state, message) {
    
        if (!state) state = {};

        const currentList = state[message.persistenceId];

        if (!currentList) {
            state[message.persistenceId] = {}
        };

        if (!state[message.persistenceId][message.author]) {
            state[message.persistenceId][message.author] = [];
        }

        const keyList = state[message.persistenceId][message.author];

        message.keys.forEach(keyInfo => {
            const startSequenceNr = keyInfo.startSequenceNr;
            const key = keyInfo.key;

            const alreadyExists = keyList.find((keyInfo) =>  keyInfo.startSequenceNr == startSequenceNr);

            if (!alreadyExists) {
                keyList.push({
                    startSequenceNr,
                    key
                })
            }
        })

        return state;
    }

    function getAllKeysFor (persistenceId, authorId) {
        const getState = promisify(index.get);

        return getState().then(state => {
            
            state = state || [];
            const keys = state[persistenceId];

            if (!keys) {
                return [];
            } else if (!keys[authorId]) {
                return [];
            } else {
                return keys[authorId];
            }
        })
    }

    return {
        getAllKeysFor: (persistenceId, authorId) => getAllKeysFor(persistenceId, authorId),
        /**
         * Get the latest key for a given persistence ID that we are writing with
         */
        getMyCurrentKeyFor: (persistenceId) => {
            return getAllKeysFor(persistenceId, myKey).then(
                allKeys => {
                    return allKeys[allKeys.length -1]
                }
            )
        }
    }
}
