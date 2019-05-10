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

const makeTestMessage = require('./testUtils').makeTestMessage;

function createSbot(testBotName, keys) {

    var makeSbot = CreateTestSbot.use(
        require('ssb-private')
    )
    .use(        require('ssb-query')    )
    .use(require('../index'))

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
                const test = sbot.akkaPersistenceIndex.events.eventsByPersistenceId(pietPubWithPrefix, "sample-id-6", 0, 30);
                pull(test, pull.collect((err, result) => {

                    if (err) {
                        assert.fail(err);
                    } else {
                        assert(100, result.length, 100);

                        result.forEach((item, iteration) => {
                            assert.equal(item.sequenceNr, iteration + 1, "The result list should be ordered.");

                        })

                    }

                    sbot.close()

                }));
            }
        )
    })

    describe("Splits test messages into parts, then reassembles them", function() {

        const sbot = createSbot("test5", pietKeys);
        
        const randomData = crypto.randomBytes(22000).toString('hex');

        const payload = {
            'data': randomData
        }

        const testMessage = makeTestMessage(payload,1, 'test-id');

        sbot.akkaPersistenceIndex.events.persistEvent(testMessage, (err, result) => {

            if (err) {
                console.log(err);
                assert.fail(err);
            } else {

                const source = sbot.akkaPersistenceIndex.events.eventsByPersistenceId(pietPubWithPrefix, "test-id", 0, 10);

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

        const persistEvent = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

        Promise.all([persistEvent(longTestMessage), persistEvent(shortTestMessage), persistEvent(longTestMessage2)]).then(
            () => {

                const source = sbot.akkaPersistenceIndex.events.eventsByPersistenceId(pietPubWithPrefix, "test-id", 0, 10);

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

    describe('If there are only some parts for a message replicated so far, we don\'t return any of them', function() {
        const sbot = createSbot("test7", pietKeys);

        const postMessage = bluebird.promisify(sbot.publish);

        postMessage(
            {
                "payload": {
                    "data": "new-test-" + 1
                  },
                  "sequenceNr": 1,
                  "persistenceId": "sample-id-7",
                  "manifest": "org.openlaw.scuttlebutt.persistence.Evt",
                  "deleted": false,
                  "sender": null,
                  "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
                  "type": "akka-persistence-message"
                }
        ).then(() => {
            return [1,2,3].map((number) => {

                const payload = {
                    "payload": {
                        "data": "new-test-" + number
                      },
                      "sequenceNr": 2,
                      "part": number,
                      "of": 7,
                      "persistenceId": "sample-id-7",
                      "manifest": "org.openlaw.scuttlebutt.persistence.Evt",
                      "deleted": false,
                      "sender": null,
                      "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
                      "type": "akka-persistence-message"
                    }
    
                    return postMessage(payload);
            });
        })
        .then(results => Promise.all(results))
        .then(() => {
            const source = sbot.akkaPersistenceIndex.events.eventsByPersistenceId(pietPubWithPrefix, "sample-id-7", 0, 10);

            pull(source, pull.collect((err, result) => {

                if (err) {
                    console.log(err);
                    assert.fail(err);
                } else {
                    assert.equal(result.length, 1, "There should only be one result");
                }

                sbot.close();
            }))
        })

    });

    describe("Can get all the events for an author", function () {

        const sbot = createSbot("test9", pietKeys);

        postTestMessages(sbot, "sample-id-7").then( () => {
            const source = sbot.akkaPersistenceIndex.events.allEventsForAuthor('@' + pietKeys.public, {
                start: 0,
                end: 10
            });

            pull(source, pull.collect((err, result) => {
                assert.equal(result.length, 10, "There should be 10 results")

                result.forEach((item, num) => {
                    assert.equal(item.sequenceNr, num + 1, "Should have expected sequence number.")
                })

                const source2 = sbot.akkaPersistenceIndex.events.allEventsForAuthor('@' + pietKeys.public, {
                    start: 11,
                    end: 21
                });
    
                pull(source2, pull.collect((err, result) => {
                    assert.equal(result.length, 10, "There should be 10 results");
    
                    result.forEach((item, num) => {
                        assert.equal(item.sequenceNr, num + 12, "Should have the expected sequence number.")
                    });
        
                }))

                const source3 = sbot.akkaPersistenceIndex.events.allEventsForAuthor('@' + pietKeys.public, {
                    start: 11000,
                    end: 21000
                });

                pull(source3, pull.collect((err, result) => {
                    assert.equal(result.length, 0, "Should have no items");

                    sbot.close();
                }))

            }));

        });

    });


});

function postTestMessages(sbot, persistenceId) {
    persistenceId = persistenceId || "sample-id-6";

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

