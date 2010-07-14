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

var redis = new (require('ngv/redis').Redis) ();

exports.setUp = function () {

	redis.connect();

	assert.isTrue(redis.select(TEST_DB_NUMBER_FOR_MOVE));
	assert.strictEqual(redis.dbsize(), 0);

	assert.isTrue(redis.select(TEST_DB_NUMBER));
	assert.strictEqual(redis.dbsize(), 0);

};

exports.tearDown = function () {

	assert.isTrue(redis.flushdb());
	assert.strictEqual(redis.dbsize(), 0);
};


exports.testSet = function() {
	assert.isTrue(redis.set('foo', 'bar'));
};

exports.testSetNX = function() {
    assert.isTrue(redis.set('foo', 'bar'));
	assert.strictEqual(redis.setnx('foo', 'quux'), 0);  // fails when already set
	assert.strictEqual(redis.setnx('boo', 'apple'), 1 );  // no such key already so OK
};


exports.testGet = function() {
   assert.isTrue(redis.set('foo', 'bar'));
   assert.equal(redis.get('foo'), 'bar');
	assert.notEqual(redis.get('foo'), 'apple');
	assert.isNull(redis.get('notthere'));
};


