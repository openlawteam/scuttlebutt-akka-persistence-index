

module.exports.makeTestMessage = function makeTestMessage(data, sequenceNr, persistId) {
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

module.exports.makeRandomMessages = (persistId, numMessages) => {

    var results = [];

    for (var i = 0 ; i < numMessages ; i++) {
        var data = {
            "test": "test " + i
        }

        const result = module.exports.makeTestMessage(data, i + 1, persistId);

        results.push(result);
    }

    return results;
}