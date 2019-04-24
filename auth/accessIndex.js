const FlumeviewReduce = require('flumeview-reduce');

const promisify = require('bluebird').promisify;

const constants = require('./constants');

/**
 * An index of who we have given access to a given private entity
 */
module.exports = (sbot, myIdent) => {

    const indexVersion = 1;

    const index = sbot._flumeUse('akka-persistence-access-index', FlumeviewReduce(indexVersion, reduce, map));
    
    function map(message) {

        const messageType = message.value.content.type;
        const persistenceId = message.value.content.persistenceId;

        const authorIsMe = message.value.author === myIdent;

        if (!authorIsMe) {
            return ;
        } else if (messageType === constants.grantAccessMessageType) {

            const userId = message.value.content.userId;

            return {
                userId,
                persistenceId,
                grant: true,
                revoke: false
            }

        } else if (messageType === constants.revokeAccessMessageType) {
            const userId = message.value.content.userId;

            return {
                userId,
                persistenceId,
                grant: false,
                revoke: true
            }
        }

    }

    function reduce(state, message) {

        if (!state) {
            state = {};
        }

        const persistenceIdAccessList = state[message.persistenceId] || [];

        if (message.grant) {
            persistenceIdAccessList.push(message.userId);
        }
        else if (message.revoke) {
            var index = persistenceIdAccessList.findIndex(item => item === message.userId);
            if (index) {
                persistenceIdAccessList.splice(index, 1);
            }
        }

        state[message.persistenceId] = persistenceIdAccessList;

        return state;
    }

    return {
        usersWhoHaveAccessTo: (persistenceId) => {
            const get = promisify(index.get);

            return get().then(
                indexMap => {
                    indexMap = indexMap || {};
                    const accessList = indexMap[persistenceId];
                    return accessList || [];
                }
            )
        }
    }
}