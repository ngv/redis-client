// redis.js - a Redis client for RingoJS server-side JavaScript
//
// Please review the Redis command reference and protocol specification:
//
// http://code.google.com/p/redis/wiki/CommandReference
// http://code.google.com/p/redis/wiki/ProtocolSpecification
//
// This implementation should make for easy maintenance given that Redis
// commands follow only a couple of conventions.  To add support for a new
// command, simply add the name to either 'inlineCommands' or 'bulkCommands'
// below.
//
// Replies are handled generically and shouldn't need any updates unless Redis
// adds a completely new response type (other than status code, integer, error,
// bulk, and multi-bulk).  See http://code.google.com/p/redis/wiki/ReplyTypes
//
// To learn more about RingoJS, see http://ringojs.org.
//
// Maksim Lin <maksim.lin@ngv.vic.gov.au>

export('Redis');

var log = require('ringo/logging').getLogger(module.id);

function debug(data) {
	include('ringo/shell');
    writeln(data);
	//log.debug(data);
}

var {PrintWriter, BufferedReader, InputStreamReader} = Packages.java.io;
var {Charset} = Packages.java.nio.charset;
var {StringBuilder} = Packages.java.lang;

var _out, _inp;
var multiBulkCount;

var CRLF = "\r\n";
var CR = 13;
var LF = 10;
var CRLF_LENGTH = 2;
var RES_TYPE_OFFSET = 1;

/**
 *
 * @param host
 * @param port
 * @returns
 */
function Redis(host, port) {
    this.port = port || 6379;
    this.host = host || '127.0.0.1';
}


/**
 * Create a connection to the db
 * and assign listeners to our connection
 * events
 *
 * If we created this connection as a result of a
 * Redis command, run the command as a callback
 * when connection is created.
 **/
Redis.prototype.connect = function() {

  log.debug(">Creating a new connection!");

  // create connection and set encoding to UTF8
  if (this._conn) {
      this.quit();
  }

  this._conn = new Packages.java.net.Socket(this.host, this.port);

  this._out = new PrintWriter(this._conn.getOutputStream(), true);
  this._inp = new BufferedReader(new InputStreamReader(this._conn.getInputStream(), Charset.forName("UTF-8")));
}

/**
 * Close connection to the Redis server
 */
Redis.prototype.quit = function() {
  if (!this._conn || !this._conn.isConnected()) {
      fatal("connection is not open");
  }
  log.debug('> quit');
  this._out.write('quit' + CRLF);
  this._conn.close();
  this._conn = null;
};

Redis.prototype.toString = function () {
    return "[Redis ]";
}


// Commands supported by Redis
// Note: 'sort' and 'quit' are handled as special cases.

var commands = {
  auth:1,        get:1,         mget:1,        incr:1,        incrby:1,
  decr:1,        decrby:1,      exists:1,      del:1,         type:1,
  keys:1,        randomkey:1,   rename:1,      renamenx:1,    dbsize:1,
  expire:1,      ttl:1,         llen:1,        lrange:1,      ltrim:1,
  lindex:1,      lpop:1,        rpop:1,        scard:1,       sinter:1,
  sinterstore:1, sunion:1,      sunionstore:1, smembers:1,    select:1,
  move:1,        flushdb:1,     flushall:1,    save:1,        bgsave:1,
  lastsave:1,    shutdown:1,    info:1,        ping:1,

  set:1,         getset:1,      setnx:1,       rpush:1,       lpush:1,
  lset:1,        lrem:1,        sadd:1,        srem:1,        smove:1,
  sismember:1,

  zadd:1, zrem:1, zscore:1, zcard:1, zincrby: 1
};



function fatal(errorMessage) {
  log.error("\n\nFATAL: " + errorMessage + "\n");

  //close off conn so anything bad still in it is not kept for next cmd to redis
  this._conn.close();
  this._conn = null;
  throw errorMessage;
}

function maybeConvertToNumber(str) {
  if (/^\s*\d+\s*$/.test(str))
    return parseInt(str, 10);

  if (/^\s*\d+\.(\d+)?\s*$/.test(str))
    return parseFloat(str);

  return str;
}

// Format an inline redis command.
// See http://code.google.com/p/redis/wiki/ProtocolSpecification#Simple_INLINE_commands

function formatInline(commandName, commandArgs, argCount) {
  var str = commandName;

  for (var i = 0; i < argCount; ++i)
    str += ' ' + commandArgs[i];

  return str + CRLF;
}

// Format a bulk redis command.
// e.g. lset key index value => lset key index value-length\r\nvalue\r\n
// where lset is commandName; key, index, and value are commandArgs
// See http://code.google.com/p/redis/wiki/ProtocolSpecification#Bulk_commands

function formatBulk(commandName, commandArgs, argCount) {
  var args = commandName;

  for (var i = 0; i < argCount - 1; ++i) {
    var val = typeof(commandArgs[i]) != 'string'
      ? commandArgs[i].toString()
      : commandArgs[i];

    args += ' ' + val;
  }

  var lastArg = typeof(commandArgs[argCount - 1]) != 'string'
    ? commandArgs[ar/gCount - 1].toString()
    : commandArgs[argCount - 1];

  var cmd = args + ' ' + lastArg.length + CRLF + lastArg + CRLF;

  return cmd;
}

/**
 * As of redis version 1.1, we can send all commands using the multibulk cmd protocol
 * to ensure binary safe strings and proves a single consistent cmd sending protocol
 * for all redis commands.
 *
 * @param commandName
 * @param commandArgs
 * @param argCount
 * @returns
 */
function formatMultiBulkCommand(commandName, commandArgs, argCount) {

	var cmd = new StringBuilder(100);

	cmd.append('*'+(argCount+1)+CRLF);
	cmd.append("$"+commandName.length+CRLF);
	cmd.append(commandName+CRLF);
	for(var i = 0; i < argCount; i++) {
		cmd.append('$'+commandArgs[i].toString().length+CRLF);
		cmd.append(commandArgs[i].toString()+CRLF);
	}
	return cmd.toString();
}

// Creates a function to send a command to the redis server.
function createCommandSender(commandName) {
  return function() {

    this.currentCmd = commandName;
    var commandArgs = arguments;

    if (!this._conn || !this._conn.isConnected()) {
      log.debug('Connection is not open - attempting to reopen');
      this.connect(); //FIXME: need to rem host & port if they are set so they are used here
    }

    // Format the command and send it.
    var cmd;

    //if (inlineCommands[commandName]) {
    //  cmd = formatInline(commandName, commandArgs, commandArgs.length);
    //} else if (bulkCommands[commandName]) {
    if (commands[commandName]) {
      cmd = formatMultiBulkCommand(commandName, commandArgs, commandArgs.length);
    } else {
      fatal('unknown command ' + commandName);
    }

    log.debug('> ' + cmd);

    this._out.print(cmd);
    this._out.flush();

    return processResponse(this._inp, this.currentCmd);
  };
}

// Create command senders for all commands
for (var commandName in commands) {
    Redis.prototype[commandName] = createCommandSender(commandName);
}

/**
 *
 * @param {BufferedReader} responseBuffer response from redis server
 * @returns {Object} a string or array of Strings with result
 */
function processResponse(responseBuffer, currentCmd) {

  // Read first char of Redis response to find out type of response?
  var typePrefix =  String.fromCharCode(responseBuffer.read());
  if ('-+$*:'.indexOf(typePrefix) < 0) {
      fatal("invalid Redis type Prefix:"+typePrefix+" responseBuffer:"+responseBuffer.readLine());
  }
  log.debug('<type char:'+typePrefix);

  var handler = replyPrefixToHandler[typePrefix];

  return postProcessResults(handler(responseBuffer), currentCmd);
}


function handleBulkReply(responseBuffer) {

  var valueLength = parseInt(responseBuffer.readLine(), 10);

  if (valueLength == -1) {
      return null;
  }

  var buf = java.lang.reflect.Array.newInstance(java.lang.Character.TYPE, valueLength);

  responseBuffer.read(buf);

  responseBuffer.read();responseBuffer.read(); //read out trailing CR+LF

  var s = String(java.lang.String(buf));
  log.debug("<s:"+s);
  return s;
}

function handleMultiBulkReply(responseBuffer) {

  var count = parseInt(responseBuffer.readLine(), 10);

  log.debug('> Multibulk count is (' + count + ')');

  if (count === -1) {
      return [ null ];
  }

  var entries = [];

  for (var i = 0; i < count; ++i) {
    responseBuffer.read(); //swallow intial "4" as handleBulkReply expects this
    var bulkReply = handleBulkReply(responseBuffer);
    entries = entries.concat(bulkReply);
  }
  return entries;
}

function handleSingleLineReply(responseBuffer) {
  var line = responseBuffer.readLine();

  // Most single-line replies are '+OK' so convert such to a true value.
  if (line === 'OK') {
      value = true;
  } else {
      var value = line;
  }
  return value;
}

function handleIntegerReply(responseBuffer) {
  return parseInt(responseBuffer.readLine(), 10);
}

function handleErrorReply(responseBuffer) {
  var line = responseBuffer.readLine();
  var errorMessage = (line.indexOf("ERR ") != 0)
    ? ("something bad happened: " + line)
    : line.substring(4, line.length);

  throw new Error(errorMessage);
}


// See http://code.google.com/p/redis/wiki/ReplyTypes
var replyPrefixToHandler = {
  '$': handleBulkReply,
  '*': handleMultiBulkReply,
  '+': handleSingleLineReply,
  ':': handleIntegerReply,
  '-': handleErrorReply
};

// INFO output is an object with properties for each server metadatum.
// KEYS output is a list (which is more intuitive than a ws-delimited string).

function postProcessResults(result, cmd) {

  switch (cmd) {
  case 'info':
    var infoObject = {};

    result.split('\r\n').forEach(function(line) {
      var parts = line.split(':');
      if (parts.length == 2) {
    	  infoObject[parts[0]] = maybeConvertToNumber(parts[1]);
      }
    });

    result = infoObject;
    break;

  case 'lastsave':
    result = maybeConvertToNumber(result);
    break;


  case 'setnx':
  case 'sadd':
  case 'sismember':
  case 'zadd':
  case 'zrem':
	  result = (result === 0) ? false : true;
	  break;

  default:
    break;
  }

  return result;
}


/**
 * Read this first: http://code.google.com/p/redis/wiki/SortCommand
 * options is an object which can have the following properties:
 *
 * 'byPattern': 'pattern'
 * 'limit': [start, numberOfResults]
 * 'getPatterns': [ 'pattern', 'pattern', ... ]
 * 'ascending': true|false
 * 'lexicographically': true|false
 */

exports.Redis.prototype.sort = function(key, options) {
	if (!this._conn || !this._conn.isConnected()) {
      debug('Connection is not open - attempting to reopen');
      this.connect();
    }

  var cmd = 'sort ' + key;

  if (typeof(options) == 'object') {
    var optBy = options.byPattern ? ('by ' + options.byPattern) : '';

    var optGet = '';
    if (options.getPatterns) {
      options.getPatterns.forEach(function(pat) {
        optGet += 'get ' + pat + ' ';
      });
    }

    var optAsc   = options.ascending         ? ''      : 'desc';
    var optAlpha = options.lexicographically ? 'alpha' : '';

    var optLimit = options.limit
      ? 'limit ' + options.limit[0] + ' ' + options.limit[1]
      : '';

    cmd += ' ' + optBy    + ' ' +
                 optLimit + ' ' +
                 optGet   + ' ' +
                 optAsc   + ' ' +
                 optAlpha + ' ' + CRLF;

    cmd = cmd.replace(/\s+$/, '') + CRLF;
  }

  log.debug('> ' + cmd);

  this.currentCmd = cmd;
  this._out.print(cmd);
  this._out.flush();

  return processResponse(this._inp, this.currentCmd);
};


