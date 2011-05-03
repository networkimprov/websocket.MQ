
module.exports = MqClient;

var net = require('net');
var WsStream = require('./wsstream/wsstream');

function packMsg(iJso, iData) {
  var aReq = JSON.stringify(iJso);
  var aLen = (aReq.length.toString(16)+'   ').slice(0,4);
  var aBuf = new Buffer(aLen.length + aReq.length + (iData ? iData.length : 0));
  aBuf.write(aLen, 0);
  aBuf.write(aReq, aLen.length);
  if (iData)
    iData.copy(aBuf, aLen.length + aReq.length, 0);
  return aBuf;
}

function unpackMsg(iMsg) {
  var aLen = iMsg.toString('ascii',0,4);
  var aJsEnd = parseInt(aLen, 16) +4;
  if (aJsEnd === NaN)
    throw new Error('invalid length prefix '+aLen);
  var aReq = JSON.parse(iMsg.toString('ascii', 4, aJsEnd));
  aReq._buf = iMsg.length > aJsEnd ? iMsg.slice(aJsEnd, iMsg.length) : undefined;
  return aReq;
}

function MqClient() {
  this.socket = new net.Stream;
  this.ws = new WsStream(this.socket);
  this.host = null;
  this.port = 0;

  var that = this;
  this.socket.on('connect', function() {
    that.socket.setNoDelay();
    that.event_connect();
  });
  this.socket.on('close', function() {
    if (that.event_close)
      that.event_close();
  });
  this.socket.on('error', function(err) {
    var aTime = 60;
    switch(err.errno) {
    case process.ECONNRESET:
    case process.EPIPE:
      aTime = 3;
      // fall thru
    case process.EAGAIN:
    case process.ECONNREFUSED:
      setTimeout(function() {
        that.socket.connect(that.port, that.host);
      }, aTime*1000);
      // fall thru
    case process.ENOTCONN:
      console.log('mqclient '+err.message+(err.errno === process.ENOTCONN ? '' : '. retrying...'));///debugging
      break;
    default:
      that.event_error(err.message);
    }
  });
  this.ws.on('data', function(frame, buf) {
    try {
    var aReq = unpackMsg(buf);
    if (!that['event_'+aReq.op])
      throw new Error('no handler for '+aReq.op);
    } catch (err) {
      that.event_error(err.message);
      return;
    }
    switch (aReq.op) {
    case 'registered': that['event_'+aReq.op](aReq.aliases);                            break;
    case 'deliver':    that['event_'+aReq.op](aReq.id, aReq.from, aReq._buf, aReq.etc); break;
    case 'ack':        that['event_'+aReq.op](aReq.id, aReq.type);                      break;
    case 'info':       that['event_'+aReq.op](aReq.info);                               break;
    case 'quit':       that['event_'+aReq.op](aReq.info);                               break;
    }
  });
  this.ws.on('end', function(ok) {
    if (that.event_end)
      that.event_end(ok);
  });
}

MqClient.packMsg = packMsg;
MqClient.unpackMsg = unpackMsg;

MqClient.prototype = {

  event_error: function(msg) { throw new Error(msg) } ,

  connect: function(iHost, iPort, iCallback) {
    this.host = iHost;
    this.port = iPort;
    this.on('connect', iCallback);
    this.socket.connect(iPort, iHost);
  } ,

  close: function() {
    this.ws.end();
    //this.socket.destroy();
  } ,

  isOpen: function() {
    return this.socket.readable && this.socket.writable;
  } ,

  register: function(iNode, iPassword, iAliases) {
    var aMsg = packMsg({op:'register', nodeid:iNode, password:iPassword, aliases:iAliases});
    this.ws.write(1, 'binary', aMsg);
  } ,

  login: function(iNode, iPass) {
    var aMsg = packMsg({op:'login', nodeid:iNode, password:iPass});
    this.ws.write(1, 'binary', aMsg);
  } ,

  listEdit: function(iTo, iType, iMember, iId, iEtc, iMsg) {
    var aMsg = packMsg({op:'listEdit', to:iTo, type:iType, member:iMember, id:iId, etc:iEtc}, iMsg);
    this.ws.write(1, 'binary', aMsg);
  } ,

  post: function(iToList, iMsg, iId, iEtc) {
    var aMsg = packMsg({op:'post', to:iToList, id:iId, etc:iEtc}, iMsg);
    this.ws.write(1, 'binary', aMsg);
  } ,

  ping: function(iAlias, iId, iEtc) {
    var aMsg = packMsg({op:'ping', alias:iAlias, id:iId, etc:iEtc});
    this.ws.write(1, 'binary', aMsg);
  } ,

  ack: function(iId, iType) {
    var aMsg = packMsg({op:'ack', id:iId, type:iType});
    this.ws.write(1, 'binary', aMsg);
  } ,

  send: function(iPackedMsg) {
    this.ws.write(1, 'binary', iPackedMsg);
  } ,

  on: function(iEvt, iFn) {
    if (typeof iEvt !== 'string')
      throw new Error('not a string '+iEvt);
    if (typeof iFn !== 'function')
      throw new Error('not a function '+iFn);
    this['event_'+iEvt] = iFn;
  }
};

