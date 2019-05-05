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

    describe("Test authors for a given persistence ID", function() {
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
