const describe = require('mocha').describe;
const assert = require('assert');
const crypto = require('crypto');

const ssbKeys = require('ssb-keys');
const CreateTestSbot = require('scuttle-testbot');

const pietKeys = ssbKeys.generate();
const katieKeys = ssbKeys.generate();

const constants = require('../constants');

const pull = require('pull-stream');

const promisify = require('bluebird').promisify;

const Bluebird = require('bluebird');

const IV_LENGTH = 16;

function createSbot(testBotName, keys) {

    var makeSbot = CreateTestSbot.use(
        require('ssb-private')
    ).use(require('../index'))

    const tempSbot = makeSbot({name: testBotName, keys});

    const piet = tempSbot.createFeed(pietKeys);
    const katie = tempSbot.createFeed(katieKeys);
    
  return tempSbot
}

describe("Test encryption and decryption functionality", function () {

    describe("Encrypts entities which have a key set.", function () {

        const sbot = createSbot("test123", pietKeys);

        const setKeysEvent = makeRandomSetKeyEvent(1);

        const nextEvent = makeEvent({
            "key": "value"
        }, 2);    

        sbot.akkaPersistenceIndex.persistEvent(setKeysEvent, (err, result) => {
            sbot.akkaPersistenceIndex.persistEvent(nextEvent, (err2, res2) => {
                if (err || err2) {
                    assert.fail(err);
                } else {
    
                    const stream = sbot.akkaPersistenceIndex.eventsByPersistenceId('@' + pietKeys.public, 'sample-id-6', 1, 100);

                    sbot.akkaPersistenceIndex.highestSequenceNumber(null, 'sample-id-6', (err, result) => {

                        if (err) {
                            assert.fail(err);
                        } else {
                            assert.equal(result, 2, "Current sequence number should be 2.");
                        }


                        pull(stream, pull.collect((err, results) => {
                            assert.equal(results.length, 2, "Should be two items in the stream.");
    
                            sbot.close();
                        }));

                    });

                }
            });

        });

    });

    describe("Continue to decrypt after key changes", function() {

        const sbot = createSbot("test10", pietKeys);
        const postEvent = promisify(sbot.akkaPersistenceIndex.persistEvent);

        const setKeysEvent = makeRandomSetKeyEvent(1);

        const event1 = makeEvent({
            "test": "test1"
        }, 2);

        // Change the keys half way through...
        const setKeysEvent2 = makeRandomSetKeyEvent(3);

        const event2 = makeEvent({
            "test": "test2"
        }, 4)

        const events = [setKeysEvent, event1, setKeysEvent2, event2];

        const posted = Bluebird.each(events, event => {
            return postEvent(event);
        });

        posted.then(() => {

            const stream = sbot.akkaPersistenceIndex.eventsByPersistenceId('@' + pietKeys.public, 'sample-id-6', 1, 100);

            pull(stream, pull.collect((err, result) => {

                assert.equal(result.length, 4, "there should be 4 events.");

                sbot.close();

            }));

        }).catch(err => console.log(err));
        
    })

});

function makeRandomSetKeyEvent(sequenceNr) {

    const ENCRYPTION_KEY = crypto.randomBytes(20).toString('hex');
    const SALT = 'somethingrandom';

    const key = crypto.pbkdf2Sync(ENCRYPTION_KEY, SALT, 10000, 32, 'sha512')

    const buffer = Buffer.from(key, 'base64');
    const keyBase64 = buffer.toString('base64');

    const setKeysEvent = {
        "payload": {
          },
          "sequenceNr": sequenceNr,
          "persistenceId": "sample-id-6",
          "manifest": constants.setKeyType,
          "deleted": false,
          "sender": null,
          "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
          "type": "akka-persistence-message"
        }

    return setKeysEvent;
}

function makeEvent(payload, sequenceNumber) {
    return {
        "payload": {
            payload
            },
            "sequenceNr": sequenceNumber,
            "persistenceId": "sample-id-6",
            "manifest": "random.class.name",
            "deleted": false,
            "sender": null,
            "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
            "type": "akka-persistence-message"
    }


}