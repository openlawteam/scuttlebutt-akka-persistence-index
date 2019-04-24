const ssbKeys = require('ssb-keys');
const KeysIndex = require('./keyIndex');
const AccessIndex = require('./accessIndex');
const constants = require('./constants');
const R = require('ramda');

const promisify = require('bluebird').promisify;

exports.manifest = {
    'sendKeys': 'async',
    'sendUpdatedKey': 'async',
    'getAllKeysFor': 'async',
    'getMyCurrentKeyFor': 'async'
};

module.exports = (sbot, config) => {

    const maxUsersPrivateMessagesAtOnce = 7;

    const myKey = '@' + config.keys.public;

    const keysIndex = KeysIndex(sbot, myKey);
    const accessIndex = AccessIndex(sbot, myKey);

    function sendKeys(userIds, persistenceId, keys) {

        const content = {
            "type": constants.updateKeyMessageType,
            "persistenceId": persistenceId,
            "keys": keys
        }

        const publishPrivate = promisify(sbot.private.publish);

        return publishPrivate(content, userIds);
    }

    function sendKeysToUsers(users, persistenceId, sequenceNr, newKeys) {
        // TODO: What if we crash before we've sent the new key to all the users who have been given
        // access?

        // If we're not already in the access list, we add ourselves - otherwise the entity wouldn't be
        // visible to us
        if (!users.includes(myKey)) {
            users.push(myKey);
        }

        const keys = [{
            key: newKeys,
            startSequenceNr: sequenceNr
        }];

        const chunks = R.splitEvery(maxUsersPrivateMessagesAtOnce, users);

        const sendResults = chunks.map(usersChunk => sendKeys(usersChunk, persistenceId, keys));
        return Promise.all(sendResults);
    }

    return {
        /**
         * Shares the updated access key to the list of users who are indexed to be granted access to
         * the given persistence ID. Note: the access list always contains ourselves.
         */
        sendUpdatedKey: (persistenceId, sequenceNr, newKeys) => {
            return accessIndex.usersWhoHaveAccessTo(persistenceId).then(results => {
                return sendKeysToUsers(results, persistenceId, sequenceNr, newKeys);
            });
        },
        /**
         * Send the current keys for the given persistence ID to the specified user only.
         */
        sendKeys: (userId, persistenceId) => {
            const currentKeysPromise = keysIndex.getAllKeysFor(persistenceId, myKey);

            return currentKeysPromise.then(currentKeys => {

                // We don't want to exceed the maximum message size so we send only a few at once
                // TODO: calculate exactly how many we can send at once
                const groupedKeys = R.splitEvery(4, currentKeys);

                // We add our own key so that we can keep track of who we've sent a key to
                const results = groupedKeys.map(keys => sendKeys([userId, myKey], persistenceId, keys));

                return Promise.all(results);
            })

        },

        /**
         * Gets the keys for a given persistence ID that the current user has access to. This is a list of sequence numbers and key pairs, 
         * and the range that the key applies to. e.g.
         * 
         *  [{
         *  startSequenceNr: 0,
         *  key: {}
         * }]
         */
        getAllKeysFor: (persistenceId, authorId) => {
            return keysIndex.getAllKeysFor(persistenceId, authorId);
        },

        /**
         * Get the most up to date key for the given persistence ID.
         */
        getMyCurrentKeyFor: (persistenceId) => {
            return keysIndex.getMyCurrentKeyFor(persistenceId);
        }

    }


}