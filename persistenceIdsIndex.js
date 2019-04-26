const FlumeReduce = require('flumeview-reduce');
const pull = require('pull-stream');

module.exports = (ssb, myKey) => {

    const indexVersion = 1;

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

        if (item.value.author === myKey) {
            return item.value.content['persistenceId'];
        }
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

