
module.exports = MqClient;

var net = require('net');
var WsStream = require('./wsstream/wsstream');

function packMsg(iJso, iData) {
  var aEtc = 'etc' in iJso ? JSON.stringify(iJso.etc) : '';
  if (aEtc.length)
    iJso.etc = aEtc.length;
  var aReq = JSON.stringify(iJso);
  var aLen = (aReq.length.toString(16)+'   ').slice(0,4);
  var aBuf = new Buffer(aLen.length + aReq.length + aEtc.length + (iData ? iData.length : 0));
  aBuf.write(aLen, 0);
  aBuf.write(aReq, aLen.length);
  aBuf.write(aEtc, aLen.length + aReq.length);
  if (iData)
    iData.copy(aBuf, aLen.length + aReq.length + aEtc.length, 0);
  return aBuf;
}

function unpackMsg(iMsg) {
  var aLen = iMsg.toString('ascii',0,4);
  var aJsEnd = parseInt(aLen, 16) +4;
  if (aJsEnd === NaN || aJsEnd < 4 || aJsEnd > iMsg.length)
    throw new Error('invalid length prefix '+aLen);

  var aReq = JSON.parse(iMsg.toString('ascii', 4, aJsEnd));

  if (typeof aReq.etc === 'number') {
    if (aReq.etc < 0 || aJsEnd+aReq.etc > iMsg.length)
      throw new Error('invalid etc value');
    aJsEnd += aReq.etc;
    aReq.etc = JSON.parse(iMsg.toString('ascii', aJsEnd-aReq.etc, aJsEnd));
  } else {
    delete aReq.etc;
  }
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
    case process.ENOTCONN:
      console.log('mqclient '+err.message+'. retrying in '+aTime);///debugging
      setTimeout(function() {
        try {
        that.socket.connect(that.port, that.host);
        } catch (err) {
          console.log('mqclient error on retry: '+err.message);
        }
      }, aTime*1000);
      break;
    default:
      that.event_error(err.message);
    }
  });

  this.ws.on('data', function(frame, buf) {
    try {
    var aReq = unpackMsg(buf);
    if (typeof aReq.op !== 'string' || typeof that.kParams[aReq.op] === 'undefined')
      throw new Error('invalid request op: '+aReq.op);
    for (var a in that.kParams[aReq.op])
      if (typeof aReq[a] !== that.kParams[aReq.op][a])
        throw new Error(aReq.op+' request missing param '+a);
    } catch (err) {
      that.event_error(err);
      return;
    }
    var aFn = that['event_'+aReq.op];
    switch (aReq.op) {
    case 'registered': aFn(aReq.etc, aReq.id, aReq.error);           break;
    case 'added':      aFn(aReq.offset, aReq.error);                 break;
    case 'listEdited': aFn(aReq.id, aReq.from, aReq.etc);            break;
    case 'deliver':    aFn(aReq.id, aReq.from, aReq._buf, aReq.etc); break;
    case 'ack':        aFn(aReq.id, aReq.type, aReq.error);          break;
    case 'info':       aFn(aReq.info);                               break;
    case 'quit':       aFn(aReq.info);                               break;
    }
  });

  this.ws.on('end', function(ok) {
    if (that.event_end)
      that.event_end(ok);
  });
}

MqClient.packMsg = packMsg;
MqClient.unpackMsg = unpackMsg;

  MqClient.prototype.kParams = {
    registered: {  },
    added:      {  },
    listEdited: { id:'string', from:'string', etc:'object' },
    deliver:    { id:'string', from:'string' },
    ack:        { id:'string', type:'string' },
    info:       { info:'string' },
    quit:       { info:'string' }
  };

  MqClient.prototype.event_error = function(msg) { throw new Error(msg) };

  MqClient.prototype.connect = function(iHost, iPort, iCallback) {
    this.host = iHost;
    this.port = iPort || 8008;
    this.on('connect', iCallback);
    this.socket.connect(this.port, this.host);
  };

  MqClient.prototype.close = function() {
    this.ws.end();
    //this.socket.destroy();
  };

  MqClient.prototype.isOpen = function() {
    return this.socket.readable && this.socket.writable;
  };

  MqClient.prototype.register = function(iUid, iNewNode, iAliases) {
    var aMsg = packMsg({op:'register', userId:iUid, newNode:iNewNode, aliases:iAliases});
    this.ws.write(1, 'binary', aMsg);
  };

  MqClient.prototype.addNode = function(iUid, iNewNode, iPrevNode) {
    var aMsg = packMsg({op:'addNode', userId:iUid, newNode:iNewNode, prevNode:iPrevNode});
    this.ws.write(1, 'binary', aMsg);
  };

  MqClient.prototype.login = function(iUid, iNode) {
    var aMsg = packMsg({op:'login', userId:iUid, nodeId:iNode});
    this.ws.write(1, 'binary', aMsg);
  };

  MqClient.prototype.listEdit = function(iTo, iType, iMember, iId, iEtc) {
    var aMsg = packMsg({op:'listEdit', to:iTo, type:iType, member:iMember, id:iId, etc:iEtc}, null);
    this.ws.write(1, 'binary', aMsg);
  };

  MqClient.prototype.post = function(iToList, iMsg, iId, iEtc) {
    var aMsg = packMsg({op:'post', to:iToList, id:iId, etc:iEtc}, iMsg);
    this.ws.write(1, 'binary', aMsg);
  };

  MqClient.prototype.ping = function(iAlias, iId, iEtc) {
    var aMsg = packMsg({op:'ping', alias:iAlias, id:iId, etc:iEtc});
    this.ws.write(1, 'binary', aMsg);
  };

  MqClient.prototype.ack = function(iId, iType) {
    var aMsg = packMsg({op:'ack', id:iId, type:iType});
    this.ws.write(1, 'binary', aMsg);
  };

  MqClient.prototype.send = function(iPackedMsg) {
    this.ws.write(1, 'binary', iPackedMsg);
  };

  MqClient.prototype.on = function(iEvt, iFn) {
    if (typeof iEvt !== 'string')
      throw new Error('not a string '+iEvt);
    if (typeof iFn !== 'function')
      throw new Error('not a function '+iFn);
    this['event_'+iEvt] = iFn;
  };

