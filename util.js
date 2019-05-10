const pull = require('pull-stream');
const zip  = require('pull-zip');

module.exports.isPersistenceMessage = function isPersistenceMessage(message) {
    const type = message.value.content.type;
    return type === "akka-persistence-message";
}

module.exports.takeRange = function takeRange(stream, start, end) {
    start = start || 0;

    let i = -1;
    const infiniteStream = pull.infinite(() => {
        i = i + 1;
        return i;
    });

    return pull(
        zip([infiniteStream, stream]),
        pull.filter(item => item[0] >= start),
        pull.take(result => {
            const current = result[0];
            
            const item = result[1];
            return item && (current < end || !end);
        }),
        pull.map(result => result[1])
    );
}