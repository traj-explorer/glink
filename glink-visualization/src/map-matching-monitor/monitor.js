/**
 * @author Yu Liebing
 */

if (process.argv.length !== 3 && (process.argv.length !== 4 ||
		(process.argv[2] !== 'buffer=true' && process.argv[3] !== 'buffer=false'))) {
	console.log("usage: monitor <port> [buffer=<true/false>, default: true]");
	process.exit(1);
}
const buffer = process.argv.length < 4 ? true : process.argv[3] === 'buffer=true';

const kafka = require("kafka-node")
const path = require('path')
const express = require('express')
const app = express();
const http = require('http').Server(app);
const io = require('socket.io')(http);

let objects = [];

// kafka
let conn = {'kafkaHost': '127.0.0.1:9092'};
let consumers = [
	{
		'type': 'consumer',
		'options': {'autoCommit': false, 'fromOffset': true},
		'name': 'common',
		'offset': 0,
		'topic':[
			{'topic': 'map-matching-result-viz'}
		]
	}
];

let MQ = function() { }

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

let mq = new MQ();

mq.AddConsumer(conn, consumers[0].topic, consumers[0].options, function(message) {
	let update = JSON.parse(message['value']);
	if (update == null) return
	console.log(typeof update)
	let id = update['id'];

	if (buffer) {
		console.log('insert object ' + id);
		objects.push(update);
	}

	io.emit('message', update);
});

app.use(express.static(path.join(__dirname, './public')));

app.get('/', function(req, res) {
    res.sendfile('index.html');
});

app.get('/messages', function(req, res) {
    res.sendfile('index.html');
});

io.on('connection', function(socket) {
	console.log('client connected');

	if (buffer) {
		for (var id in objects) {
			// console.log("id", id)
			socket.emit('message', objects[id]);
		}
    }
    
    socket.on('disconnect', function() {
        console.log('client disconnected');
    });
});

process.on('SIGINT', function() {
        console.log('shut down');
        process.exit();
});

if (buffer) {
	console.log('connect and listen to tracker (buffered) ...');
} else {
	console.log('connect and listen to tracker (unbuffered) ...');
}

http.listen(process.argv[2], function() {
    console.log('listening on *:' + process.argv[2]);
});
