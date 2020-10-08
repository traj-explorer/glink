const kafka = require('kafka-node');

let conn = {'kafkaHost':'127.0.0.1:9092'};
let consumers = [
    {
        'type': 'consumer',
        'options': {'autoCommit': false},
        'name':'common',
        'topic':[
            {'topic': 'mapmatching1', 'partition': 0}
        ]
    }
];

let MQ = function(){

}

MQ.prototype.AddConsumer = function (conn, topics, options, handler){
    let client = new kafka.KafkaClient(conn);
    let consumer = new kafka.Consumer(client, topics, options);

    if(!!handler){
        consumer.on('message', handler);
    }

    consumer.on('error', function(err){
        console.error('consumer error ',err.stack);
    });
}

var mq = new MQ();


mq.AddConsumer(conn, consumers[0].topic, consumers[0].options, function (message){
    console.log(message.value);
});