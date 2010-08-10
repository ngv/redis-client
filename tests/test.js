// Test suite for node (ECMAScript) redis client.
//
// NOTE: you must have started a redis-server instance on (the default)
// 127.0.0.1:6379 prior to running this test suite.
//
// NOTE: this test suite uses databases 14 and 15 for test purposes! It will
// **clear** this database at the start of the test runs.  If you want to use
// a different database number, update TEST_DB_NUMBER* below.
//

var TEST_DB_NUMBER = 15;
var TEST_DB_NUMBER_FOR_MOVE = 14;

var assert = require('assert');

var redis = new (require('ngv/redis').Redis)();

exports.setUp = function() {

	redis.connect();

	assert.isTrue(redis.select(TEST_DB_NUMBER_FOR_MOVE));
	assert.strictEqual(redis.dbsize(), 0);

	assert.isTrue(redis.select(TEST_DB_NUMBER));
	assert.strictEqual(redis.dbsize(), 0);

};

exports.tearDown = function() {

	assert.isTrue(redis.flushdb());
	assert.strictEqual(redis.dbsize(), 0);
};

exports.testSelect = function() {
	assert.isTrue(redis.set('foo', 'bar'));
	assert.isTrue(redis.select(TEST_DB_NUMBER_FOR_MOVE));
	assert.equal(redis.dbsize(), 0);

	assert.isTrue(redis.select(TEST_DB_NUMBER));
	assert.equal(redis.dbsize(), 1);
}

exports.testSet = function() {
	assert.isTrue(redis.set('foo', 'bar'));
};

exports.testSetNX = function() {
	assert.isTrue(redis.set('foo', 'bar'));
	assert.isFalse(redis.setnx('foo', 'quux')); // fails when already set
	assert.isTrue(redis.setnx('boo', 'apple')); // no such key already so OK
};

exports.testZCommands = function() {
	assert.isTrue(redis.zadd('foo', 2, 'bar'));
	assert.isFalse(redis.zadd('foo', 3, 'bar'));
	assert.equal(redis.zscore('foo', 'bar'), 3);
	assert.equal(redis.zcard('foo'), 1);

	assert.isTrue(redis.zadd('foo', 1, 'abc'));
	assert.equal(redis.zcard('foo'), 2);

	assert.isTrue(redis.zrem('foo', 'abc'));
	assert.equal(redis.zcard('foo'), 1);

	assert.equal(redis.zscore('foo', 'bar'), 3);
	assert.equal(redis.zincrby('foo', 1, 'bar'), 4);

};

exports.testGet = function() {
	assert.isTrue(redis.set('foo', 'bar'));
	assert.equal(redis.get('foo'), 'bar');
	assert.notEqual(redis.get('foo'), 'apple');
	assert.isNull(redis.get('notthere'));
};

exports.testMget = function() {
	assert.isTrue(redis.set('foo', 'bar'));
	assert.isTrue(redis.set('boo', 'apple'));

	var values = redis.mget('foo', 'boo');
	assert.equal('bar', values[0]);
	assert.equal('apple', values[1]);
}

exports.testGetSet = function() {
	assert.isTrue(redis.set('foo', 'bar'));
	var prevValue = redis.getset('foo', 'fuzz');
	assert.equal('bar', prevValue);
}

exports.testInfo = function() {
	var info = redis.info();
	// The INFO command is special; its output is parsed into an object.

	assert.isTrue(info instanceof Object);

	assert.isTrue(info.hasOwnProperty('redis_version'));
	assert.isTrue(info.hasOwnProperty('connected_clients'));
	assert.isTrue(info.hasOwnProperty('uptime_in_seconds'));

	// Some values are always numbers. Our redis client
	// will magically (ahem) convert these strings to actual
	// number types. Make sure it does this.

	assert.equal(typeof (info.uptime_in_seconds), 'number');
	assert.equal(typeof (info.connected_clients), 'number');
}

exports.testSadd = function() {
	// create set0
	assert.isTrue(redis.sadd('set0', 'member0'));

	// fails since it's already a member
	assert.isFalse(redis.sadd('set0', 'member0'));
}

exports.testIncr = function() {

	assert.equal(redis.incr('counter'), 1);
	assert.equal(redis.incr('counter'), 2);
}

exports.testSismember = function() {

	assert.isTrue(redis.sadd('set0', 'member0'));

	assert.isTrue(redis.sismember('set0', 'member0'));
	assert.isFalse(redis.sismember('set0', 'member1'));
}

exports.testSort = function() {
	assert.isTrue(redis.sadd('set0', 'member0'));
	assert.isTrue(redis.sadd('set0', 'member1'));
	assert.isTrue(redis.sadd('set0', 'member2'));
	assert.isTrue(redis.sadd('set0', 'member3'));

	var sorted = redis.sort('set0', {});
	assert.equal(sorted.length, 4);
	assert.equal(sorted.constructor, Array);

	assert.equal(redis.sort('set0', {
		limit : [ 0, 2 ]
	}).length, 2);

	var lexSorted = redis.sort('set0', {
		lexicographically : true
	});
	assert.equal(lexSorted[0], 'member3');
	assert.equal(lexSorted[3], 'member0');

}

exports.testCharset = function() {
	assert.isTrue(redis.set('utftest', 'b£a'));
	assert.equal(redis.get('utftest'), 'b£a');
};

exports.testFindCRLF = function() {
  var testStream = new Packages.java.io.StringBufferInputStream('testfind\r\n');
  assert.equal(require('ngv/redis').readToCRLF(testStream), 'testfind');
};