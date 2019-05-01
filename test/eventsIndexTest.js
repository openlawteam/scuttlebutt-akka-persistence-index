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
const promisify = bluebird.promisify;

const crypto = require('crypto');


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

    describe("Splits test messages into parts, then reassembkes them", function() {

        const sbot = createSbot("test5", pietKeys);
        
        const randomData = crypto.randomBytes(22000).toString('hex');

        const payload = {
            'data': randomData
        }

        const testMessage = makeTestMessage(payload,1, 'test-id');

        sbot.akkaPersistenceIndex.persistEvent(testMessage, (err, result) => {

            if (err) {
                console.log(err);
                assert.fail(err);
            } else {

                const source = sbot.akkaPersistenceIndex.eventsByPersistenceId(pietPubWithPrefix, "test-id", 0, 10);

                pull(source, pull.collect((err, result) => {

                    if (err) {
                        assert.fail(err);
                    } else {

                        assert.equal(result.length, 1, "Should be re-assembled into 1 message.");

                        const data = result[0].payload.data;

                        assert.equal(data, payload.data, "Should be re-assembled properly");

                        pull(sbot.messagesByType({type: 'akka-persistence-message'}), pull.collect((err, result2) => {

                            assert.equal(result2.length, 7, "Should be more than one message in the actual database.");

                            sbot.close()

                        }))

                    }
                }))
            }
        });
    })

    describe('A message in some parts followed by a whole message followed by parts results in 3 messages', function () {
        const sbot = createSbot("test6", pietKeys);
        
        const randomData = crypto.randomBytes(22000).toString('hex');
        const randomData2 = crypto.randomBytes(22000).toString('hex');

        const longPayload = {
            'data': randomData
        }

        const longPayload2 = {
            'data': randomData2
        }

        const shortPayload = {
            'data': 'hola'
        };

        const longTestMessage = makeTestMessage(longPayload,1, 'test-id');
        const shortTestMessage = makeTestMessage(shortPayload, 2, 'test-id');
        const longTestMessage2 = makeTestMessage(longPayload2, 3, 'test-id');

        const persistEvent = promisify(sbot.akkaPersistenceIndex.persistEvent);

        Promise.all([persistEvent(longTestMessage), persistEvent(shortTestMessage), persistEvent(longTestMessage2)]).then(
            () => {

                const source = sbot.akkaPersistenceIndex.eventsByPersistenceId(pietPubWithPrefix, "test-id", 0, 10);

                pull(source, pull.collect((err, result) => {
                    if (err) {
                        assert.fail(err);
                    } else {
                        assert.equal(result.length, 3, "There should be 3 results");
                        assert.equal(result[0].payload.data, randomData, "The first long payload should be as expected.");

                        assert.equal(result[1].payload.data, "hola", "The second result should be the small message.");
                        assert.equal(result[2].payload.data, randomData2, "The second long payload should be as expected.");
                    }

                    sbot.close();

                }))


            }
        ).catch((err) => {

            sbot.close();
            assert.fail(err);
        })


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

function makeTestMessage(data, sequenceNr, persistId) {
    return {
        "payload": data,
          "sequenceNr": sequenceNr,
          "persistenceId": persistId,
          "manifest": "org.openlaw.scuttlebutt.persistence.Evt",
          "deleted": false,
          "sender": null,
          "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
          "type": "akka-persistence-message"
        };

}

