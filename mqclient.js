
module.exports = MqClient;

var WebSocket = require('./websocket-client').WebSocket;

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

function unpackMsg(iMsg, iClient) {
  var aLen = iMsg.toString('ascii',0,4);
  try {
  var aJsEnd = parseInt(aLen, 16) +4;
  if (aJsEnd === NaN)
    throw new Error('invalid length prefix '+aLen);
  var aReq = JSON.parse(iMsg.toString('ascii', 4, aJsEnd));
  if (!iClient['event_'+aReq.op])
    throw new Error('no handler for '+aReq.op);
  } catch (err) {
    iClient.event_error(err.message);
    return;
  }
  var aBuf = iMsg.length > aJsEnd ? iMsg.slice(aJsEnd, iMsg.length) : null;
  switch (aReq.op) {
  case 'registered': iClient['event_'+aReq.op](aReq.aliases);                       break;
  case 'deliver':    iClient['event_'+aReq.op](aReq.id, aReq.from, aBuf, aReq.etc); break;
  case 'ack':        iClient['event_'+aReq.op](aReq.id, aReq.type);                 break;
  case 'info':       iClient['event_'+aReq.op](aReq.info);                          break;
  case 'quit':       iClient['event_'+aReq.op](aReq.info);                          break;
  }
}

function MqClient() {
  this.ws = null;
  this.event_error = function(msg) { throw new Error(msg) };
}

MqClient.packMsg = packMsg;

MqClient.prototype = {

  connect: function(iWhere, iCallback) {
    this.ws = new WebSocket(iWhere);
    var that = this;
    this.ws.addListener('open',  iCallback);
    this.ws.addListener('close', function() {
      if (that.event_close)
        that.event_close();
      that.ws = null;
    });
    this.ws.addListener('data', function(buf) {
      unpackMsg(buf, that);
    });
    this.ws.addListener('wserror', function(err) {
      if (that.ws && !that.ws.socketError) throw err;
      console.log(err.message);
      that.close();
    });
  } ,

  close: function() {
    if (this.ws)
      this.ws.close();
  } ,

  isOpen: function() {
    return this.ws !== null && this.ws.readyState === this.ws.OPEN && this.ws.writeable;
  } ,

  register: function(iNode, iPassword, iAliases) {
    var aMsg = packMsg({op:'register', nodeid:iNode, password:iPassword, aliases:iAliases});
    this.ws.send(aMsg);
  } ,

  login: function(iNode, iPass) {
    var aMsg = packMsg({op:'login', nodeid:iNode, password:iPass});
    this.ws.send(aMsg);
  } ,

  post: function(iToList, iMsg, iId, iEtc) {
    var aMsg = packMsg({op:'post', to:iToList, id:iId, etc:iEtc}, iMsg);
    this.ws.send(aMsg);
  } ,

  ping: function(iAlias, iId, iEtc) {
    var aMsg = packMsg({op:'ping', alias:iAlias, id:iId, etc:iEtc});
    this.ws.send(aMsg);
  } ,

  ack: function(iId, iType) {
    var aMsg = packMsg({op:'ack', id:iId, type:iType});
    this.ws.send(aMsg);
  } ,

  send: function(iPackedMsg) {
    this.ws.send(iPackedMsg);
  } ,

  on: function(iEvt, iFn) {
    if (typeof iEvt !== 'string')
      throw new Error('not a string '+iEvt);
    if (typeof iFn !== 'function')
      throw new Error('not a function '+iFn);
    this['event_'+iEvt] = iFn;
  }
};

