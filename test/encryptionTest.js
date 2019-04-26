const describe = require('mocha').describe;
const assert = require('assert');
const crypto = require('crypto');

const ssbKeys = require('ssb-keys');
const CreateTestSbot = require('scuttle-testbot');

const pietKeys = ssbKeys.generate();
const katieKeys = ssbKeys.generate();

const constants = require('../constants');

const pull = require('pull-stream');

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

        const iv = crypto.randomBytes(16);

        const base64Iv = iv.toString('base64');

        const ENCRYPTION_KEY = 'Must256bytes(32characters)secret';
        const SALT = 'somethingrandom';

        const key = crypto.pbkdf2Sync(ENCRYPTION_KEY, SALT, 10000, 32, 'sha512')

        const buffer = Buffer.from(key, 'base64');
        const keyBase64 = buffer.toString('base64');

        const setKeysEvent = {
            "payload": {
                "sequenceNr": 1,
                "key": {
                    "iv": base64Iv,
                    "key": keyBase64
                }

              },
              "sequenceNr": 1,
              "persistenceId": "sample-id-6",
              "manifest": constants.setKeyType,
              "deleted": false,
              "sender": null,
              "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
              "type": "akka-persistence-message"
            }

            const nextEvent = {
                "payload": {
                    "random": "stuff",
                    },
                    "sequenceNr": 2,
                    "persistenceId": "sample-id-6",
                    "manifest": "random.class.name",
                    "deleted": false,
                    "sender": null,
                    "writerUuid": "b73a85f3-8ca5-49ad-8405-9b5d886703e2",
                    "type": "akka-persistence-message"
            }

        

        sbot.akkaPersistenceIndex.persistEvent(setKeysEvent, (err, result) => {
            sbot.akkaPersistenceIndex.persistEvent(nextEvent, (err2, res2) => {
                if (err || err2) {
                    assert.fail(err);
                } else {
    
                    const stream = sbot.akkaPersistenceIndex.eventsByPersistenceId('@' + pietKeys.public, 'sample-id-6', 1, 100);
    
                    pull(stream, pull.collect((err, results) => {
                        assert.equal(results.length, 2, "Should be two items in the stream.");

                        sbot.close();
                    }));
    
                }
            });

        });

    })

});