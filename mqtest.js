
var sys = require('sys');
var WebSocket = require('./websocket-client').WebSocket;

function makeMsg(iJso, iData) {
  var aReq = JSON.stringify(iJso);
  var aLen = (aReq.length.toString(16)+'   ').slice(0,4);
  var aBuf = new Buffer(aLen.length + aReq.length + (iData ? iData.length : 0));
  aBuf.write(aLen, 0);
  aBuf.write(aReq, aLen.length);
  if (iData)
    iData.copy(aBuf, aLen.length + aReq.length, 0);
  return aBuf;
}

sToList = { aabba:true, bbccb:true, ccddc:true, ddeed:true, eeffe:true, ffggf:true, gghhg:true, hhiih:true, iijji:true, jjkkj:true };
sMsgList = [ 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten' ];
for (var a=0; a < sMsgList.length; ++a)
  sMsgList[a] = new Buffer(sMsgList[a]);

function Testconn(iId) {
  this.link = null;
  this.id = iId;
  this.open = false;
  this.data = {};
}

Testconn.prototype = {
  recv: function(iMsg) {
    var that = this;
    if (!that.open) {
      console.log('on closed conn: '+iMsg);
      return;
    }
    var aLen = iMsg.toString('ascii',0,4);
    if (/^[0-9A-F]/.test(aLen)) {
      var aJsEnd = parseInt(aLen, 16) +4;
      var aReq = JSON.parse(iMsg.toString('ascii', 4, aJsEnd));
      var aBuf = iMsg.length > aJsEnd ? iMsg.toString('ascii', aJsEnd,iMsg.length) : null;
      if (aReq.op === 'deliver') {
        var aT = Date.now() % 10;
        var aLink = that.link;
        setTimeout(function() {
          if (that.link === aLink)
            that.link.send(makeMsg({op:'ack', type:'ok', id:aReq.id}));
        }, aT*10);
        if (aBuf in that.data)
          ++that.data[aBuf];
        else
          that.data[aBuf] = 1;
        if (that.data[aBuf] % 10 === 0)
          console.log(that.id+' got 10 '+aBuf);
      } else if (aReq.op === 'ack') {
        that.ack[+aReq.id] = true;
      } else if (aReq.op === 'quit') {
        console.log('quit, '+aReq.info);
      } else
        console.log(sys.inspect(aReq));
    } else
      console.log(iMsg);
  } ,

  connect: function(iCallback) {
    this.open = true;
    this.link = new WebSocket('ws://localhost:8008/');
    var that = this;
    this.link.addListener('open', iCallback);
    this.link.addListener('data', function(buf) { that.recv(buf) });
    this.link.addListener('close', function() { that.open = false; that.link = null; });
    this.link.addListener('wserror', function(err) { throw err });
    this.ack = [];
    this.ack.length = sMsgList.length;
  } ,

  close: function() {
    for (var a=0, aTot=0; a < this.ack.length; ++a)
      if (this.ack[a]) ++aTot;
    console.log(this.id+' '+aTot+' ackd');
    this.open = false;
    this.link.close();
    this.link = null;
  }
}

function testLink(aC, iState) {
  switch (iState) {
  case 0:
    aC.connect(function() {
      aC.link.send(makeMsg({op:'login', nodeid:aC.id}));
      setTimeout(testLink, (Date.now()%10)*1000, aC, iState+1);
    });
    break;
  case 1: case 2: case 3: case 4: case 5: case 6: case 7: case 8: case 9: case 10:
    if (aC.link)
      aC.link.send(makeMsg({op:'post', to:sToList, id:(iState-1).toString()}, sMsgList[iState-1]));
    setTimeout(testLink, (Date.now()%10)*800, aC, aC.link ? iState+1 : 0);
    break;
  case 11:
    if (aC.link)
      aC.close();
    setTimeout(testLink, (Date.now()%10)*800, aC, 0);
    break;
  }
}

for (var a in sToList) {
  testLink(new Testconn(a), 0);
}

