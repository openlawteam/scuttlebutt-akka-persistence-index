const crypto = require('crypto');
const promisify = require('bluebird').promisify;
const pull = require('pull-stream');

const constants = require('./constants');
const PersistenceIdsIndex = require('./persistenceIdsIndex');
const EntityEventsIndex = require('./entityEventsIndex');
const AccessIndex = require('./auth/index');

const Defer = require('pull-defer')

exports.name = 'akka-persistence-index'

exports.version = require('./package.json').version

exports.manifest = {
    currentPersistenceIds: 'source',
    currentPersistenceIdsAsync: 'async',
    livePersistenceIds: 'source',

    eventsByPersistenceId: 'source',
    highestSequenceNumber: 'async',
    persistEvent: 'async'
}

const indexVersion = 1;

exports.init = (ssb, config) => {

    const IV_LENGTH = 16;

    const encryptionAlgorithm = 'aes-256-ctr';

    const persistenceIdsIndex = PersistenceIdsIndex(ssb, '@' + config.keys.public);

    const accessIndex = AccessIndex(ssb, config);
    const entityEventsIndex = EntityEventsIndex(ssb, '@' + config.keys.public);

    const publishPublic = promisify(ssb.publish);

    const randomBytes = promisify(crypto.randomBytes);

    /**
     * The decrypted stream of events persisted for the given entity ID, up to the last sequence number visible
     * to the user.
     * 
     * @param {*} authorId the author of the events (or ourselves if null.)
     * @param {*} persistenceId the peristence ID of the entity
     * @param {*} fromSequenceNumber the start sequence number to stream from
     * @param {*} toSequenceNumber the maximum sequence number to stream up to (stream ends early if the last item has a smaller
     * sequence number than this.)
     */
    function eventsByPersistenceId(authorId, persistenceId, fromSequenceNumber, toSequenceNumber) {
        authorId = authorId || '@' + config.keys.public;

        const encryptedSource = entityEventsIndex.eventsByPersistenceId(authorId, persistenceId, fromSequenceNumber, toSequenceNumber);

        const decryptionThrough = Defer.through();

        const keysForPersistenceId = accessIndex.getAllKeysFor(persistenceId, authorId || '@' + config.keys.public);

        keysForPersistenceId.then(keys => {
            const through = pull.map(message => decrypt(keys, message));
            decryptionThrough.resolve(through);
        });


        return pull(encryptedSource, decryptionThrough, pull.filter(decrypted => decrypted != null));
    }

    function decrypt(keyList, message) {

        if (!message.encrypted) {
            return message;
        } else {
            const sequenceNr = message.sequenceNr;
            const keyInfo = getKeyForSequenceNr(keyList, sequenceNr);
            if (!keyInfo) {
                // If we don't have a key for it, return null to indicate it can't be decrypted
                return null;
            }

            const nonceLength = keyInfo.key.nonceLength;
            const nonce = Buffer.alloc(nonceLength);

            const keyBase64 = keyInfo.key.key;
            const key = Buffer.from(keyBase64, 'base64');

            const bytes = Buffer.from(message.payload, 'base64');
            bytes.copy(nonce, 0, 0, nonceLength);

            const iv = Buffer.alloc(IV_LENGTH);
            
            nonce.copy(iv, 0, 0, nonceLength)

            const encryptedText = bytes.slice(nonceLength)

            const decipher = crypto.createDecipheriv(encryptionAlgorithm, key, iv);
            const decryptedText = Buffer.concat([decipher.update(encryptedText), decipher.final()]).toString();

            try {
                const payloadObj = JSON.parse(decryptedText);
                message.payload = payloadObj;
                return message;
            } catch (ex) {
                // We may not have been given the necessary keys (having been removed from the access list.)
                return null;
            }
        }

    }

    function getKeyForSequenceNr(keyList, sequenceNr) {
        const result = keyList.find( (key, index) => {
            const keyLessThanSequence = key.startSequenceNr <= sequenceNr;
            const nextKeySequenceNrItem = keyList[index + 1] ? keyList[index + 1].startSequenceNr : null;
            return keyLessThanSequence && nextKeySequenceNrItem == null || (nextKeySequenceNrItem > sequenceNr);         
        });

        return result;
    }

    /**
     * Returns the highest sequence number of an entity by sequence number for
     * an entity someone has authored
     * 
     * @param {*} authorId the author of the events (or ourselves if null.)
     * @param {*} persistenceId the persistence ID of the entity
     */
    function highestSequenceNumber(authorId, persistenceId, cb) {
        authorId = authorId || '@' + config.keys.public;

        return entityEventsIndex.highestSequenceNumber(authorId, persistenceId, cb);
    }

    /**
     * Persists the message to the log.
     * 
     * If the persisted message is one which adds a user to the access list (AddAccess),
     * then as well as persisting this message to the log, a private message is sent to
     * the user with the scuttlebutt key in the AddAccess payload with the list of decryption
     * keys and the sequence number they apply to.
     * 
     * The private message is sent using ssb-private (https://github.com/ssbc/ssb-private)
     * 
     * Note: if this payload exceeds the maximum message length (if the key has changed frequently),
     * we will have to send it as several private messages instead. Example payload:
     * 
     * {
     *    type: "akka-persistence-keys",
     *    content: {
     *    persistenceId: <persistence ID of the entity>
     *    keys: [
     *     {
     *       startSequenceNumber: 0,
     *       key: "..."
     *     },
     *     {
     *       startSequenceNumber: 10,
     *       key: "..."
     *     }
     *    ]
     *   }
     *
     * }
     * 
     * If the message removes someone from the access list, then the key is changed and all the remaining
     * people with access are private messaged the new key with the same schema as above, but with only
     * one item (the new key) in the keys field.
     * 
     * @param {*} persistedMessage expected to follow the schema of a PersistentRepr in akka
     * (https://doc.akka.io/japi/akka/current/akka/persistence/PersistentRepr.html)
     */
    function persistEvent(persistedMessage, cb) {

        if (validateMessage(persistedMessage, cb)) {
            if (persistedMessage.manifest === constants.setKeyType) {

                const keyInfo = {
                    key: generateKeyBase64(),
                    nonceLength: 8
                }

                accessIndex.sendUpdatedKey(
                    persistedMessage.persistenceId,
                    persistedMessage.sequenceNr,
                    keyInfo,
                ).then(
                    // We encrypt this message with the new key, after it's been indexed.
                    () => publishWithKey(persistedMessage)
                ).asCallback(cb);
    
            } else if (persistedMessage.manifest === constants.addUserType) {
                const userId = persistedMessage.payload.userId;
                const persistenceId = persistedMessage.persistenceId;

                accessIndex.sendKeys(userId, persistenceId)
                    .then(() => accessIndex.trackAddUser(persistenceId, userId))
                    .then(() => publishWithKey(persistedMessage))
                    .asCallback(cb);

            } else if (persistedMessage.manifest === constants.removeUserType) {
                const userId = persistedMessage.payload.userId;
                const persistenceId = persistedMessage.persistenceId;
                const newKey = persistedMessage.payload.newKey;
                const sequenceNr = persistedMessage.sequenceNr;
                
                // Todo: validate the above fields aren't null.

                accessIndex.trackRemoveUser(persistenceId, userId)
                    .then(() =>
                        accessIndex.sendUpdatedKey(persistenceId, sequenceNr, newKey)
                    ).then(
                        () => publishWithKey(persistedMessage)
                    ).asCallback(cb);
    
            } else {
                publishWithKey(persistedMessage).asCallback(cb);
            }
        }
    }

    function validateMessage(persistedMessage, cb) {
        return true;
    }

    function publishWithKey(persistedMessage) {
        const currentKey = accessIndex.getMyCurrentKeyFor(persistedMessage.persistenceId);
        persistedMessage['type'] = "akka-persistence-message";

        return currentKey.then(key => {

            if (key == null) {
                // This is not an encrypted / private entity, we publish it in plain text.
                
                return publishPublic(persistedMessage);
            } else {

                const payloadAsJsonText = JSON.stringify(persistedMessage.payload);

                return encryptWithKey(payloadAsJsonText, key).then(cypherText => {
                    persistedMessage['payload'] = cypherText;
                    persistedMessage['encrypted'] = true;
    
                    return publishPublic(persistedMessage);
                })
            }

        });
    }

    function encryptWithKey(payload, keyInfo) {
        const key = keyInfo.key.key;
        const nonceLength = keyInfo.key.nonceLength;

        return randomBytes(nonceLength).then(nonceBytes => {
            const keyBytes = Buffer.from(key, 'base64');

            const ivBytes = Buffer.alloc(IV_LENGTH)
            nonceBytes.copy(ivBytes);
    
            const cipher = crypto.createCipheriv(encryptionAlgorithm, keyBytes, ivBytes);
    
            const payloadAsString = JSON.stringify(payload);
    
            const bytes = Buffer.concat([nonceBytes, cipher.update(payloadAsString), cipher.final()])
    
            return bytes.toString('base64');
        })
    }

    function generateKeyBase64() {
        const ENCRYPTION_KEY = crypto.randomBytes(20).toString('hex');
        const SALT = crypto.randomBytes(16);
    
        const buffer = crypto.pbkdf2Sync(ENCRYPTION_KEY, SALT, 10000, 32, 'sha512')

        return buffer.toString('base64');
    }

    return {
        eventsByPersistenceId: eventsByPersistenceId,
        highestSequenceNumber: highestSequenceNumber,
        persistEvent: persistEvent,

        currentPersistenceIds: () => {
            return persistenceIdsIndex.currentPersistenceIds();
        },
        currentPersistenceIdsAsync: (cb) => {
            return persistenceIdsIndex.currentPersistenceIdsAsync(cb);
        },
        livePersistenceIds: () => {
            return persistenceIdsIndex.livePersistenceIds();
        }
    }

}