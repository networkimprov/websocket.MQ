
var sys = require('sys');
var MqClient = require('./mqclient');

sToList = { aabba:true, bbccb:true, ccddc:true, ddeed:true, eeffe:true, ffggf:true, gghhg:true, hhiih:true, iijji:true, jjkkj:true };
var sListAgent, sListList = { thelist:2 };
sMsgList = [ 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten' ];
for (var a=0; a < sMsgList.length; ++a) {
  var aBuf = new Buffer(a*1001 || 3);
  for (var ai=0; ai < aBuf.length; ++ai)
    aBuf[ai] = 'b';
  aBuf.write(sMsgList[a]);
  sMsgList[a] = aBuf;
}
sBig = new Buffer(1*1024*1024);
for (var a=0; a < sBig.length; ++a)
  sBig[a] = 'a';

function Testconn(iId, iReg) {
  this.open = false;
  this.reg = iReg;
  this.id = iId;
  this.data = {};
  this.big = 0;
  this.ack = [];
  this.client = new MqClient();
  var that = this;
  this.client.on('registered', function(aliases) {
    that.reg = true;
    console.log(that.id+' registered '+aliases);
    that.client.login(that.id, 'node'+that.id);
  });
  this.client.on('info', function(msg) {
    console.log(that.id+' '+msg);
    if (sPw) return;
    if (!sListAgent || that.id === sListAgent) {
      sListAgent = that.id;
      for (var aId in sToList)
        if (sToList[aId] === true)
          break;
      if (sToList[aId] === true) {
        sToList[aId] = 1;
        that.client.listEdit('thelist', 'add', aId, 'list'+that.id, 'listEdit '+aId, null);
      }
    }
  });
  this.client.on('quit', function(msg) {
    console.log(that.id+' quit '+msg);
  });
  this.client.on('deliver', function(id, from, msg, etc) {
    setTimeout(function() {
      if (that.client.isOpen())
        that.client.ack(id, 'ok');
    }, (Date.now()%10)*10);
    if (msg)
      var aName = msg.toString('ascii', 0, Math.min(5, msg.length));
    if (etc) {
      console.log(etc);
    } else if (msg.length === sBig.length) {
      if (++that.big % 10 === 0)
        console.log(that.id+' got 10 big');
    } else if (aName in that.data) {
      if (++that.data[aName] % 100 === 0)
        console.log(that.id+' got 100 '+aName+' '+msg.length);
    } else
      that.data[aName] = 1;
  });
  this.client.on('ack', function(id, type) {
    if (type === 'ok') {
      if (!/^list/.test(id))
        that.ack[+id] = true;
    } else
      console.log('ack fail '+type+' '+id);
  });
  this.client.on('end', function(ok) {
    if (!ok)
      console.log('client got abrupt close');
  });
  this.client.on('close', function() {
    that.open = false;
    for (var a=0, aTot=0; a < that.ack.length; ++a)
      if (that.ack[a]) ++aTot;
    console.log(that.id+' '+aTot+' ackd');
    that.ack.length = 0;
  });
}

function testLink(aC, iState) {
  switch (iState) {
  case 0:
    if (aC.open)
      setTimeout(testLink, (Date.now()%10)*500, aC, 0);
    else
      aC.client.connect('localhost', 8008, function() {
        aC.open = true;
        if (aC.reg)
          aC.client.login(aC.id, 'node'+aC.id);
        else
          aC.client.register(aC.id, 'node'+aC.id, 'prevnode', 'alias'+aC.id);
        setTimeout(testLink, (Date.now()%10+1)*1000, aC, iState+1);
      });
    break;
  case 1: case 2: case 3: case 4: case 5: case 6: case 7: case 8: case 9: case 10:
    var aData = aC.id === 'jjkkj' && iState === 1 ? sBig : sMsgList[iState-1];
    if (aC.client.isOpen())
      if (aC.id === 'jjkkj' && iState === 2)
        aC.client.ping('aliasiijji', (iState-1).toString(), 'pingmsg');
      else
        aC.client.post(aC.id === 'iijji' && iState === 9 ? sListList : sToList, aData, (iState-1).toString());
    setTimeout(testLink, (Date.now()%10)*800, aC, aC.client.isOpen() ? iState+1 : 0);
    break;
  case 11:
    aC.client.close();
    setTimeout(testLink, (Date.now()%10)*800, aC, 0);
    break;
  }
}

try {
var sPw = require('fs').statSync('mqreg');
} catch (err) {
  if (err.errno !== process.ENOENT) throw err;
}

for (var a in sToList) {
  testLink(new Testconn(a, !!sPw), 0);
}

