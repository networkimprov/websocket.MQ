
var sys = require('sys');
var MqClient = require('./mqclient');

sToList = { aabba:true, bbccb:true, ccddc:true, ddeed:true, eeffe:true, ffggf:true, gghhg:true, hhiih:true, iijji:true, jjkkj:true };
sMsgList = [ 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten' ];
for (var a=0; a < sMsgList.length; ++a)
  sMsgList[a] = new Buffer(sMsgList[a]);
sBig = new Buffer(1*1024*1024);
for (var a=0; a < sBig.length; ++a)
  sBig[a] = 'a';

function Testconn(iId) {
  this.id = iId;
  this.data = {};
  this.big = 0;
  this.ack = [];
  this.client = new MqClient();
  this.client.on('info', function(msg) {
    console.log(msg);
  });
  this.client.on('quit', function(msg) {
    console.log('quit '+msg);
  });
  var that = this;
  this.client.on('deliver', function(id, from, msg) {
    setTimeout(function() {
      if (that.client.isOpen())
        that.client.ack(id, 'ok');
    }, (Date.now()%10)*10);
    if (msg.length > 1024) {
      if (++that.big % 10 === 0)
        console.log(that.id+' got 10 big');
    } else if (msg in that.data) {
      if (++that.data[msg] % 10 === 0)
        console.log(that.id+' got 10 '+msg);
    } else
      that.data[msg] = 1;
  });
  this.client.on('ack', function(id) {
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
        aC.client.login(aC.id);
        setTimeout(testLink, (Date.now()%10)*1000, aC, iState+1);
      });
    break;
  case 1: case 2: case 3: case 4: case 5: case 6: case 7: case 8: case 9: case 10:
    var aData = /*aC.id === 'jjkkj' && iState === 10 ? sBig :*/ sMsgList[iState-1];
    if (aC.client.isOpen())
      aC.client.post(sToList, aData, (iState-1).toString());
    setTimeout(testLink, (Date.now()%10)*800, aC, aC.client.isOpen() ? iState+1 : 0);
    break;
  case 11:
    aC.client.close();
    setTimeout(testLink, (Date.now()%10)*800, aC, 0);
    break;
  }
}

for (var a in sToList) {
  testLink(new Testconn(a), 0);
}

