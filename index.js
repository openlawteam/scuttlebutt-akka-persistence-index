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




