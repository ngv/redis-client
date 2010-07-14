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
      //log.debug(data);
    require('ringo/shell').writeln(data);
}

var {PrintWriter, BufferedReader, InputStreamReader} = Packages.java.io;
var {Charset} = Packages.java.nio.charset;

var _out, _inp;
var multiBulkCount;

var currentCmd = "";

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

  debug(">Creating a new connection!");

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
  debug('> quit');
  this._out.write('quit' + CRLF);
  this._conn.close();
  this._conn = null;
};

Redis.prototype.toString = function () {
    return "[Redis ]";
}


// Commands supported by Redis (as of June, 2009).
// Note: 'sort' and 'quit' are handled as special cases.

var inlineCommands = {
  auth:1,        get:1,         mget:1,        incr:1,        incrby:1,
  decr:1,        decrby:1,      exists:1,      del:1,         type:1,
  keys:1,        randomkey:1,   rename:1,      renamenx:1,    dbsize:1,
  expire:1,      ttl:1,         llen:1,        lrange:1,      ltrim:1,
  lindex:1,      lpop:1,        rpop:1,        scard:1,       sinter:1,
  sinterstore:1, sunion:1,      sunionstore:1, smembers:1,    select:1,
  move:1,        flushdb:1,     flushall:1,    save:1,        bgsave:1,
  lastsave:1,    shutdown:1,    info:1,        ping:1
};

var bulkCommands = {
  set:1,         getset:1,      setnx:1,       rpush:1,       lpush:1,
  lset:1,        lrem:1,        sadd:1,        srem:1,        smove:1,
  sismember:1
};



function fatal(errorMessage) {
  debug("\n\nFATAL: " + errorMessage + "\n");
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
    ? commandArgs[argCount - 1].toString()
    : commandArgs[argCount - 1];

  var cmd = args + ' ' + lastArg.length + CRLF + lastArg + CRLF;

  return cmd;
}

// Creates a function to send a command to the redis server.

function createCommandSender(commandName) {
  return function() {

     debug("this:"+this);

    this.currentCmd = commandName;
    var commandArgs = arguments;

    if (!this._conn || !this._conn.isConnected()) {
      debug('Connection is not open - attempting to reopen');
      this.connect(); //FIXME: need to rem host & port if they are set so they are used here
    }

    // Format the command and send it.
    var cmd;

    if (inlineCommands[commandName]) {
      cmd = formatInline(commandName, commandArgs, commandArgs.length);
    } else if (bulkCommands[commandName]) {
      cmd = formatBulk(commandName, commandArgs, commandArgs.length);
    } else {
      fatal('unknown command ' + commandName);
    }

    debug('> ' + cmd);

    this._out.print(cmd);
    this._out.flush();

    return processResponse(this._inp);
  };
}

// Create command senders for all commands.

for (var commandName in inlineCommands) {
    Redis.prototype[commandName] = createCommandSender(commandName);
}

for (var bulkCommand in bulkCommands) {
    Redis.prototype[bulkCommand] = createCommandSender(bulkCommand);
}



/**
 *
 * @param {BufferedReader} res response from redis server
 * @returns
 */
function processResponse(responseBuffer) {

  // Read first char of Redis response to find out type of response?
  var typePrefix =  String.fromCharCode(responseBuffer.read());
  if ('-+$*:'.indexOf(typePrefix) < 0) {
      fatal("invalid Redis type Prefix:"+typePrefix);
  }
  debug('<type char:'+typePrefix);

  var handler  = replyPrefixToHandler[typePrefix];

  if(typePrefix == '*') {
    return postProcessResults(responseBuffer);
  } else {
      return handler(responseBuffer); //TODO: processReponseEnd(handler());
  }

}


function handleBulkReply(responseBuffer) {

  var valueLength = parseInt(responseBuffer.readLine(), 10);

  if (valueLength == -1) {
      return null;
  }

  var buf = java.lang.reflect.Array.newInstance(java.lang.Character.TYPE, valueLength);

  responseBuffer.read(buf);

  responseBuffer.read();responseBuffer.read(); //read out trailing CR+LF

  var s = java.lang.String(buf);
  debug("<s:"+s);
  return [s];
}

function handleMultiBulkReply(responseBuffer) {

  var count = parseInt(responseBuffer.readLine(), 10);

  debug('> Multibulk count is (' + count + ')');

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

function handleErrorReply(reponseBuffer) {
  var line = responseBuffer.readLine();
  var errorMessage = (line.indexOf("ERR ") != 0)
    ? ("something bad happened: " + line)
    : line.substring(4, line.length - 4);

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

function postProcessResults(result) {
  switch (currentCmd) {
  case 'info':
    var infoObject = {};

    result.split('\r\n').forEach(function(line) {
      var parts = line.split(':');
      if (parts.length == 2)
        infoObject[parts[0]] = maybeConvertToNumber(parts[1]);
    });

    result = infoObject;
    break;

  case 'keys':
    result = result.split(' ');
    break;

  case 'lastsave':
    result = maybeConvertToNumber(result);
    break;

  default:
    break;
  }

  return result;
}


// Read this first: http://code.google.com/p/redis/wiki/SortCommand
// options is an object which can have the following properties:
//   'byPattern': 'pattern'
//   'limit': [start, end]
//   'getPatterns': [ 'pattern', 'pattern', ... ]
//   'ascending': true|false
//   'lexicographically': true|false

exports.sort = function(key, options, callback) {
  if (!conn.isConnected()) {
      fatal("connection is not open");
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

  if (exports.debugMode)
    debug('> ' + cmd);

  conn.send(cmd);

  //todo read and return results
};


