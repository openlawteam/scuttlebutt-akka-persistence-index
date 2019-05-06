const CreateTestSbot = require('scuttle-testbot');
const ssbKeys = require('ssb-keys');

const pietKeys = ssbKeys.generate();
const katieKeys = ssbKeys.generate();

const makeRandomPosts = require('./testUtils').makeRandomMessages;

const R = require('ramda');

const promisify = require('bluebird').promisify;

const describe = require('mocha').describe;
const assert = require('assert');

const pull = require('pull-stream');

const constants = require('../constants');

function createSbot(testBotName, keys) {

    var makeSbot = CreateTestSbot.use(
        require('ssb-private')
    ).use(require('../index'))

    const tempSbot = makeSbot({ name: testBotName, keys });

    const piet = tempSbot.createFeed(pietKeys);
    const katie = tempSbot.createFeed(katieKeys);

    return tempSbot
}

describe("Test persistence IDs indexing functionality", function () {

    describe("Return my current persistence IDs", function () {

        const sbot = createSbot("test1", pietKeys);

        const postsMade = postTestDataSet(sbot);

        postsMade.then(() => {

            sbot.akkaPersistenceIndex.persistenceIds.myCurrentPersistenceIdsAsync(
                (err, result) => {

                    if (err) {
                        console.log(err);
                        assert.fail(err);
                    } else {
                        assert.equal(result.length, 4, "there should only be 4 items.");

                        assert.equal(result[2], "persistence-id-3", "There should be the right third result")
                    }

                    sbot.close();
                }
            )
        }).catch(err => assert.fail(err));

    });

    describe("Test authors for a given persistence ID", function () {
        const sbot = createSbot("test2", pietKeys);
        const postsMade = postTestDataSet(sbot);

        postsMade.then(() => {
            const source = sbot.akkaPersistenceIndex.persistenceIds.authorsForPersistenceId("persistence-id-3");

            pull(source, pull.collect((err, result) => {

                if (err) {
                    assert.fail(err);
                } else {

                    assert.equal(result.length, 1, "There should only be one result");
                    assert.equal(result, "@" + pietKeys.public, "There should be the right author result.");
                }

                sbot.close();

            }));
        })
    });

    describe("Test persistence IDs for a given author ID", function () {
        const sbot = createSbot('test-3', pietKeys);

        const postsMade = postTestDataSet(sbot);

        postsMade.then(() => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.persistenceIdsForAuthor('@' + pietKeys.public)

            pull(source, pull.collect((err, result) => {

                if (err) {
                    console.log(err);
                    assert.fail(err);
                } else {
                    assert.equal(result.length, 4, "there should only be 4 items.");
                    assert.equal(result[2], "persistence-id-3", "There should be the right third result")
                }

                sbot.close();

            }));

        });
    })

    describe("Test all authors", function () {

        const sbot = createSbot('test-4', pietKeys);

        const postsMade = postTestDataSet(sbot);

        postsMade.then(() => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.allAuthors();

            pull(source, pull.collect((err, result) => {
                if (err) {
                    assert.fail(err);
                } else {
                    assert(result.length, 1, "There should only be one result");
                }

                sbot.close();

            }));


        });
    });

    describe("Test authorsForPersistenceId when we have encryption key for private entity.", function () {
        const sbot = createSbot('test-5', pietKeys);
        const persistMessage = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

        const setKeysEvent = {
            "payload": {
            },
            "sequenceNr": 1,
            "persistenceId": "sample-id-6",
            "manifest": constants.setKeyType,
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        }

        const setKeysEvent2 = {
            "payload": {
            },
            "sequenceNr": 1,
            "persistenceId": "sample-id-7",
            "manifest": constants.setKeyType,
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        }

        Promise.all([persistMessage(setKeysEvent), persistMessage(setKeysEvent2)]).then(result => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.authorsForPersistenceId('sample-id-6');

            pull(source, pull.collect((err, result) => {
                if (err) {
                    assert.fail(err);
                } else {
                    assert.equal(result.length, 1, "There should be one result");
                }

                sbot.close();
            }));


        })
    });

    describe("Test authorsForPersistenceId when entity is private, but we don't have the keys", function () {
        const sbot = createSbot('test-6', pietKeys);
        const persistMessage = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

        const messageWeCannotDecrypt = {
            "payload": "encrypted payload...",
            "encrypted": true,
            "sequenceNr": 1,
            "persistenceId": "sample-id-6",
            "manifest": "random.manifest",
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        };

        const messageWeCannotDecrypt2 = {
            "payload": "encrypted payload...",
            "encrypted": true,
            "sequenceNr": 2,
            "persistenceId": "sample-id-6",
            "manifest": "random.manifest",
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        };

        Promise.all([persistMessage(messageWeCannotDecrypt), persistMessage(messageWeCannotDecrypt2)]).then(() => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.authorsForPersistenceId('sample-id-6');

            pull(source, pull.collect((err, result) => {
                if (err) {
                    assert.fail(err);
                } else {
                    assert.equal(result.length, 0, "There should be no results.");
                }

                sbot.close();
            }));

        });

    });

    describe("Test persistenceIdsForAuthor when we have the encryption key for a private entity", function () {
        const sbot = createSbot('test-7', pietKeys);
        const persistMessage = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

        const setKeysEvent = {
            "payload": {
            },
            "sequenceNr": 1,
            "persistenceId": "sample-id-6",
            "manifest": constants.setKeyType,
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        };

        const setKeysEvent2 = {
            "payload": {
            },
            "sequenceNr": 1,
            "persistenceId": "sample-id-7",
            "manifest": constants.setKeyType,
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        };

        Promise.all([persistMessage(setKeysEvent), persistMessage(setKeysEvent2)]).then(() => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.persistenceIdsForAuthor('@' + pietKeys.public);

            pull(source, pull.collect((err, result) => {
                if (err) {
                    assert.fail(err);
                } else {
                    assert.equal(result.length, 2, "There should be 2 results.");
                }

                sbot.close();
            }));

        });

    });

    describe("Test persistenceIdsForAuthor when entity is private, but we don't have the keys", function () {
        
        const sbot = createSbot('test-8', pietKeys);
        const persistMessage = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

        // Encrypted, and we don't have a key for it
        const messageEvent = {
            "payload": "random encrypted text",
            "sequenceNr": 1,
            "persistenceId": "sample-id-9",
            "manifest": "random.manifest",
            "encrypted": true,
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        };

        // Encrypted, and we will give ourselves the key for it automatically.
        const setKeysEvent2 = {
            "payload": {
            },
            "sequenceNr": 1,
            "persistenceId": "sample-id-10",
            "manifest": constants.setKeyType,
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
        };

        Promise.all([persistMessage(messageEvent), persistMessage(setKeysEvent2)]).then(() => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.persistenceIdsForAuthor('@' + pietKeys.public);

            pull(source, pull.collect((err, result) => {
                if (err) {
                    assert.fail(err);
                } else {
                    assert.equal(result.length, 1, "There should only be 1 result.");
                }

                sbot.close();
            }));

        });
    });

    describe("Test pagination for persistence IDs for author", function() {

        const sbot = createSbot('test-9', pietKeys);
        const persistMessage = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

        const results = [];

        for (var i = 0; i < 100; i++) {
            const messageEvent = {
                "payload": {

                },
                "sequenceNr": 1,
                "persistenceId": "sample-id-" + i,
                "manifest": "random.manifest",
                "deleted": false,
                "sender": null,
                "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
                "type": "akka-persistence-message"
            };

            const result  = persistMessage(messageEvent);
            results.push(result);
        }

        Promise.all(results).then(() => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.persistenceIdsForAuthor(
                '@' + pietKeys.public,
                {
                    start: 0,
                    end: 20
                }
            )

            pull(source, pull.collect((err, result) => {
                assert.equal(result.length, 20, "There should be 20 results");
            }))

            const source2 = sbot.akkaPersistenceIndex.persistenceIds.persistenceIdsForAuthor(
                '@' + pietKeys.public,
                {
                    start: 99,
                    end: 120
                }
            );

            pull(source2, pull.collect((err,results) => {
                assert.equal(results.length, 1, "There should be one result");

                sbot.close();
            }));

        });

    });

    describe("Test pagination for own persistence IDs", function() {
        const sbot = createSbot('test-10', pietKeys);
        const persistMessage = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

        const results = [];

        for (var i = 0; i < 100; i++) {
            const messageEvent = {
                "payload": {

                },
                "sequenceNr": 1,
                "persistenceId": "sample-id-" + i,
                "manifest": "random.manifest",
                "deleted": false,
                "sender": null,
                "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
                "type": "akka-persistence-message"
            };

            const result  = persistMessage(messageEvent);
            results.push(result);
        }

        Promise.all(results).then(() => {

            const source = sbot.akkaPersistenceIndex.persistenceIds.myCurrentPersistenceIds(
                {
                    start: 0,
                    end: 20
                }
            )

            pull(source, pull.collect((err, result) => {
                console.log(result[0]);
                assert.equal(result[0], "sample-id-0", "Should start with th expected value.");
                assert.equal(result.length, 20, "There should be 20 results");
            }))

            const source2 = sbot.akkaPersistenceIndex.persistenceIds.myCurrentPersistenceIds(
                {
                    start: 99,
                    end: 120
                }
            );

            pull(source2, pull.collect((err,results) => {
                assert.equal(results.length, 1, "There should be one result");

                sbot.close();
            }));

        });

    });

})

function postTestDataSet(sbot) {
    const persistenceId1 = "persistence-id-1";
    const persistenceId2 = "persistence-id-2";
    const persistenceId3 = "persistence-id-3";
    const persistenceId4 = "persistence-id-4";

    const posts = [
        makeRandomPosts(persistenceId1, 10),
        makeRandomPosts(persistenceId2, 10),
        makeRandomPosts(persistenceId3, 10),
        makeRandomPosts(persistenceId4, 10)
    ];

    const allPosts = R.flatten(posts);

    const persistMessage = promisify(sbot.akkaPersistenceIndex.events.persistEvent);

    const results = allPosts.map(msg => persistMessage(msg));

    return Promise.all(results);
}
