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

export('Redis', 'readToCRLF');

var log = require('ringo/logging').getLogger(module.id);

function debug(data) {
	log.debug(data);
}

var {PrintWriter, BufferedReader, DataInputStream, DataOutputStream, OutputStreamWriter, InputStreamReader } = Packages.java.io;
var {Charset} = Packages.java.nio.charset;
var {StringBuilder} = Packages.java.lang;

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
    this.currentDB = 0;
}


/**
 * Create a connection to Redis.
 * NOTE: if already connected, the exiting connection will be closed and a new one opened.
 **/
Redis.prototype.connect = function() {

  log.debug(">Creating a new connection!");
  reconnect(this);

}

function reconnect(self) {

    if (self._conn) {
        self.quit(); //gracefully close an existing connection
    }

    self._conn = new Packages.java.net.Socket(self.host, self.port);

    self._out = new OutputStreamWriter(self._conn.getOutputStream(), Charset.forName("UTF-8"));
    self._inp = self._conn.getInputStream();
}

/**
 * Close connection to the Redis server
 */
Redis.prototype.select = function(dbId) {
  var db = parseInt(dbId);
  if (isNaN(db)) {
      throw Error('Invalid Redis db number:'+dbId);
  }
  this.currentCmd = 'select '+db;
  log.debug('> '+this.currentCmd);

  try {
      writeCmd(this, this.currentCmd);
      var result = processResponse(this);
  } catch(e) {
      //retry cmd
      log.info('retrying cmd:'+this.currentCmd);
      reconnect(this);
      writeCmd(this, this.currentCmd);
      result = processResponse(this);
  }
  return result;
};

/**
 * Close connection to the Redis server
 */
Redis.prototype.quit = function() {
  if (!this._conn || !this._conn.isConnected()) {
      this.fatal("connection is not open");
  }
  log.debug('> quit');
  writeCmd(this, 'quit');
  this._conn.close();
  this._conn = null;
};


Redis.prototype.fatal = function(errorMessage) {
  log.error("\n\nFATAL: " + errorMessage + "\n");

  //close off conn so anything bad still in it is not kept for next cmd to redis
  this._conn && this._conn.close();
  this._conn = null;
  throw new Error(errorMessage);
}


Redis.prototype.toString = function () {
    return "[Redis]";
}


// Commands supported by Redis
// Note: 'select', 'sort' and 'quit' are handled as special cases.

var commands = {
  auth:1,        get:1,         mget:1,        incr:1,        incrby:1,
  decr:1,        decrby:1,      exists:1,      del:1,         type:1,
  keys:1,        randomkey:1,   rename:1,      renamenx:1,    dbsize:1,
  expire:1,      ttl:1,         llen:1,        lrange:1,      ltrim:1,
  lindex:1,      lpop:1,        rpop:1,        scard:1,       sinter:1,
  sinterstore:1, sunion:1,      sunionstore:1, smembers:1,
  move:1,        flushdb:1,     flushall:1,    save:1,        bgsave:1,
  lastsave:1,    shutdown:1,    info:1,        ping:1,

  set:1,         getset:1,      setnx:1,       rpush:1,       lpush:1,
  lset:1,        lrem:1,        sadd:1,        srem:1,        smove:1,
  sismember:1,

  zadd:1, zrem:1, zscore:1, zcard:1, zincrby: 1
};


function maybeConvertToNumber(str) {
  if (/^\s*\d+\s*$/.test(str))
    return parseInt(str, 10);

  if (/^\s*\d+\.(\d+)?\s*$/.test(str))
    return parseFloat(str);

  return str;
}


/**
 * As of redis version 1.1, we can send all commands using the multibulk cmd protocol
 * to ensure binary safe strings and proves a single consistent cmd sending protocol
 * for all redis commands.
 *
 * @param {String} commandName
 * @param {Array} commandArgs
 * @returns
 */
function formatMultiBulkCommand(commandName, commandArgs) {

	var cmd = new StringBuilder(100);

	cmd.append('*'+(commandArgs.length+1)+CRLF);
	cmd.append("$"+commandName.length+CRLF);
	cmd.append(commandName+CRLF);


	for(var i = 0; i < commandArgs.length; i++) {
	    var asUTF8Bytes = (new Packages.java.lang.String(commandArgs[i])).getBytes('UTF-8');
		cmd.append('$'+asUTF8Bytes.length+CRLF);
		cmd.append(commandArgs[i]+CRLF);
	}
	return cmd.toString();
}

// Creates functions to send a command to the redis server,
// these will then all be made methods of the Redis obj.
function createCommandSender(commandName) {
  return function() { //this will be a Redis obj, as the function is going to be made a emthod of the Redis obj

    this.currentCmd = commandName;
    var commandArgs = arguments;

    if (!this._conn) {
      log.debug('Connection is not open - attempting to reopen');
      this.connect(this.host, this.port);
    }

    // Format the command and send it.
    var cmd;

    if (commands[commandName]) {
      cmd = formatMultiBulkCommand(commandName, commandArgs);
    } else {
      this.fatal('unknown command ' + commandName);
    }

    log.debug('> ' + cmd);

    writeCmd(this, cmd);

    var result;
    //BLOCK on trying to read first byte of the reply
    try {
    	result = processResponse(this);
    } catch(e) {
    		//retry cmd
    		log.info('retrying cmd:'+cmd);
    		reconnect(this);
    		writeCmd(this, cmd);
    		result = processResponse(this);
    }
    return result;
  };
}

/**
 *
 * @param {OutputStreamWriter} writer
 * @param {String} cmd
 * @returns
 */
function writeCmd(self, cmd) {
	var writer = self._out;
    try {
        writer.write(cmd);
        writer.flush();
    } catch(e) {
        self.fatal('Error occured processing CMD:'+cmd+': '+e);
    }
}

// Create command senders for all commands
for (var commandName in commands) {
    Redis.prototype[commandName] = createCommandSender(commandName);
}

/**
 *
 * @param {} responseStream response from redis server
 * @returns {Object} a string or array of Strings with result
 */
function processResponse(self) {

  var responseStream = self._inp;
  var firstByte;
  try {
	  firstByte = responseStream.read();
  } catch(e) {
      self.fatal("Error during responseStream.read: "+e);
  }
  if (firstByte == -1) { //redis timed our conn out for being Idle!
      throw new Error("Redis connection timeout");
  }
  //Read first char of Redis response to find out type of response?
  var typePrefix = String.fromCharCode(firstByte);
  if ('-+$*:'.indexOf(typePrefix) < 0) {
      self.fatal("invalid Redis type Prefix:"+typePrefix+
                      " byte:"+firstByte);
  }
  log.debug('<type char:'+typePrefix);

  var handler = replyPrefixToHandler[typePrefix];

  return postProcessResults(handler(responseStream), self.currentCmd);
}


function handleBulkReply(responseStream) {

  var valueLength = parseInt(readToCRLF(responseStream), 10);

  if (valueLength == -1) {
      return null;
  }

  var buf = java.lang.reflect.Array.newInstance(java.lang.Byte.TYPE, valueLength);

  responseStream.read(buf);
  responseStream.skip(2);//read out trailing CR+LF

  var s = String(Packages.java.lang.String(buf, Charset.forName("UTF-8")));
  log.debug("<s response:"+s);
  return s;
}

function handleMultiBulkReply(responseStream) {

  var count = parseInt(readToCRLF(responseStream), 10);

  log.debug('> Multibulk count is (' + count + ')');

  if (count === -1) {
      return [ null ];
  }

  var entries = [];

  for (var i = 0; i < count; ++i) {
    responseStream.skip(1); //swallow intial "4" as handleBulkReply expects this
    var bulkReply = handleBulkReply(responseStream);
    entries = entries.concat(bulkReply);
  }
  return entries;
}

function handleSingleLineReply(responseStream) {
  var line = readToCRLF(responseStream);

  // Most single-line replies are '+OK' so convert such to a true value.
  if (line === 'OK') {
      return true;
  } else {
      return line;
  }
}

function handleIntegerReply(responseStream) {
  return parseInt(readToCRLF(responseStream), 10);
}

function handleErrorReply(responseStream) {
  var line = readToCRLF(responseStream);
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
	if (!this._conn) {
      log.debug('Connection is not open - attempting to reopen');
      reconnect(this);
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
  writeCmd(this, cmd);

  //BLOCK on trying to read first byte of the reply
  try {
  	return processResponse(this);
  } catch(e) {
  		//retry cmd
  		log.info('retrying cmd:'+cmd);
  		reconnect(this);
  		writeCmd(this, cmd);
  	    return processResponse(this);
  	}
};

/**
 * Reads a stream 1 byte at a time, looking for a CRLF terminator.
 * Converts the bytes returned as a string using an ascii encoding.
 *
 * @param stream {java.io.InputStream}
 * @returns {String} text NOT including terminating CR LF characters
 */
function readToCRLF (stream) {
    var buf = new StringBuilder(50);
    var c = stream.read();
    while(c != -1) {
        buf.append(String.fromCharCode(c));
        if (buf.length() > 2) {
            var last2Chars = buf.substring(buf.length()-2, buf.length());
            if (last2Chars && last2Chars == CRLF) {
                return buf.substring(0, buf.length()-2);
            }
        }
        c = stream.read();
    }
}

