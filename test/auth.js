const CreateTestSbot = require('scuttle-testbot');

const Auth = require('../auth/index');
const ssbKeys = require('ssb-keys')
const constants = require('../auth/constants');

const describe = require('mocha').describe;
const assert = require('assert');

const pietKeys = ssbKeys.generate();
const katieKeys = ssbKeys.generate();

const pull = require('pull-stream');

const pietPubWithPrefix = '@' + pietKeys.public;

function createSbot(testBotName, keys) {

    var makeSbot = CreateTestSbot.use(
        require('ssb-private')
    )

    const tempSbot = makeSbot({name: testBotName, keys});

    const piet = tempSbot.createFeed(pietKeys);
    const katie = tempSbot.createFeed(katieKeys);
    
  return tempSbot
}

describe("Auth keys functionality", function() {
    describe('Test that updating keys gives us the keys automatically', function() {

        const sbot = createSbot("testBot1", pietKeys);

        const auth = Auth(sbot, {
            keys: {
                public: pietKeys.public
            }
        });
    
        var newKeys = ssbKeys.generate();

        var persistenceId = "test-persistence-id";
    
        const updateKeysResult = auth.sendUpdatedKey(persistenceId, 1, newKeys);
    
        var update1Finished = updateKeysResult.then(
            () => {

                return auth.getAllKeysFor(persistenceId, pietPubWithPrefix).then(
                    result => {
                        assert.equal(result.length, 1, "There should be one key");
                        assert.equal(result[0].startSequenceNr, 1, "There should be a key with start sequence number 1.");
                    }
                )
            }
    
        ).catch(err => {
            assert.fail(err)
        })

        const updateKeysResult2 = update1Finished.then(() => auth.sendUpdatedKey(persistenceId, 10, newKeys));

        updateKeysResult2.then(
            () => {
                return auth.getAllKeysFor(persistenceId, pietPubWithPrefix).then(
                    result => {
                        assert.equal(result.length, 2, "There should be two keys after an update");
                        assert.equal(result[1].startSequenceNr, 10, "There should be a 2nd key with start sequence number 10.");
                    }
                ).then(() => {

                    return auth.getMyCurrentKeyFor(persistenceId).then(
                        currentKeys => {
                            assert.equal(currentKeys.startSequenceNr, 10, "getMyCurrentKeyFor should return the latest key.");

                        }
                    )

                })

            }
        ).catch(err => {
            assert.fail(err);
        }).finally(() => {
            sbot.close()
        });

    });

});


