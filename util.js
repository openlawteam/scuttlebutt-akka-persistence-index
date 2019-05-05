
module.exports.isPersistenceMessage = function isPersistenceMessage(message) {
    const type = message.value.content.type;
    return type === "akka-persistence-message";
}