
var sys = require('sys');
var MqClient = require('./mqclient');

sToList = { aabba:true, bbccb:true, ccddc:true, ddeed:true, eeffe:true, ffggf:true, gghhg:true, hhiih:true, iijji:true, jjkkj:true };
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
  });
  this.client.on('info', function(msg) {
    console.log(that.id+' '+msg);
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
    if (type === 'fail')
      console.log('ack fail '+id);
    else
      that.ack[+id] = true;
  });
  this.client.on('close', function() {
    for (var a=0, aTot=0; a < that.ack.length; ++a)
      if (that.ack[a]) ++aTot;
    console.log(that.id+' '+aTot+' ackd');
    that.ack.length = 0;
  });
}

function testLink(aC, iState) {
  switch (iState) {
  case 0:
    if (aC.client.ws)
      setTimeout(testLink, (Date.now()%10)*500, aC, 0);
    else
      aC.client.connect('ws://localhost:8008/', function() {
        if (aC.reg)
          aC.client.login(aC.id, 'password');
        else
          aC.client.register(aC.id, 'password', 'alias'+aC.id);
        setTimeout(testLink, (Date.now()%10+1)*1000, aC, aC.reg ? iState+1 : 11);
      });
    break;
  case 1: case 2: case 3: case 4: case 5: case 6: case 7: case 8: case 9: case 10:
    var aData = aC.id === 'jjkkj' && iState === 1 ? sBig : sMsgList[iState-1];
    if (aC.client.isOpen())
      if (aC.id === 'jjkkj' && iState === 2)
        aC.client.ping('aliasiijji', (iState-1).toString(), 'pingmsg');
      else
        aC.client.post(sToList, aData, (iState-1).toString());
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

