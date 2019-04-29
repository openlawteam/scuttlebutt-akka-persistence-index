const CreateTestSbot = require('scuttle-testbot');

const ssbKeys = require('ssb-keys')
const constants = require('../auth/constants');

const describe = require('mocha').describe;
const assert = require('assert');

const pietKeys = ssbKeys.generate();
const katieKeys = ssbKeys.generate();

const pull = require('pull-stream');

const pietPubWithPrefix = '@' + pietKeys.public;

const bluebird = require('bluebird');

function createSbot(testBotName, keys) {

    var makeSbot = CreateTestSbot.use(
        require('ssb-private')
    ).use(require('../index'))

    const tempSbot = makeSbot({name: testBotName, keys});

    const piet = tempSbot.createFeed(pietKeys);
    const katie = tempSbot.createFeed(katieKeys);
    
  return tempSbot
}

describe("Entity events index", function() {

    describe("Can get stream of persisted events", function() {

        const sbot = createSbot("test4", pietKeys);

        postTestMessages(sbot).then(
            () => {
                const test = sbot.akkaPersistenceIndex.eventsByPersistenceId(pietPubWithPrefix, "sample-id-6", 0, 10);
                pull(test, pull.collect((err, result) => {

                    //console.log(result);

                    if (err) {
                        assert.fail(err);
                    } else {
                       // console.log(result);
                        assert(10, result.length, 10);
                    }

                    sbot.close()

                }));
            }
        )

    })

});

function postTestMessages(sbot) {
    var postMessage = bluebird.promisify(sbot.publish);

    var results = [];
    for (var i = 1; i < 100; i++) {
        var result = postMessage({
            "payload": {
                "data": "new-test-" + i
              },
              "sequenceNr": i,
              "persistenceId": "sample-id-6",
              "manifest": "org.openlaw.scuttlebutt.persistence.Evt",
              "deleted": false,
              "sender": null,
              "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
              "type": "akka-persistence-message"
            }
        );

        results.push(result);
    }

    return Promise.all(results);
}

