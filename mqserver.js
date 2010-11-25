
var sys = require('sys');
var fs = require('fs');

var ws = require('./websocket-server/lib/ws/server');

var sMainDir = 'mqstore/';
var sTempDir = sMainDir+'temp/';
var sMaxQuietSize = 100000;

var sQueues = {}; // array objects indexed by nodeid
var sActive = {}; // connections indexed by nodeid
var sShutdown = false;

function main(argv) {
  try {
  fs.mkdirSync(sMainDir, 0700);
  fs.mkdirSync(sTempDir, 0700);
  } catch (err) {
    if (err.errno !== process.EEXIST) throw err;
  }

  if (argv.length > 2) {
    if (argv[2] !== 'stop' && argv[2] !== 'test') {
      console.log('invalid command line argument. use stop or test.');
      return;
    }
    try {
    var aPid = fs.readFileSync(sMainDir+'.pid');
    } catch (err) {
      if (err.errno !== process.ENOENT) throw err;
      if (argv[2] === 'stop')
        console.log('no .pid file found');
      else
        test();
      return;
    }
    if (argv[2] === 'stop') {
      try {
      process.kill(+aPid, 'SIGINT');
      } catch (err) {
        fs.unlink(sMainDir+'.pid', noop);
      }
    } else {
      console.log('cannot test while server already running');
    }
    return;
  }

  var aServer = ws.createServer();

  aServer.addListener("connection", function(conn){
    var aLink = new Link(conn);

    conn.addListener("message", function(msg){
      try {
      aLink.handleMessage(new Buffer(msg));
      } catch (err) {
        conn.send(makeMsg({op:'quit', info:err}));
        conn.close();
      }
    });

    conn.addListener("close", function(){
      aLink.finalize();
    });
  });

  fs.writeFileSync(sMainDir+'.pid', process.pid.toString());
  process.on('SIGINT', function() {
    fs.unlink(sMainDir+'.pid', noop);
    aServer.close();
    for (var a in sActive)
      sActive[a].conn.close();
    sShutdown = true;
  });

  aServer.listen(8008);
}

function writeAll(iFd, iBuf, iCallback) {
  fs.write(iFd, iBuf, 0, iBuf.length, null, function (err, written) {
    if (err) return iCallback(err);
    if (written === iBuf.length)
      iCallback(null);
    else
      writeAll(iFd, iBuf.slice(written), iCallback);
  });
}

function syncFile(iPath, iCallback) {
  fs.open(iPath, 'r', function(err, fd) {
    if (err) return iCallback(err);
    fs.fsync(fd, function(err) {
      fs.close(fd);
      iCallback(err);
    });
  });
}

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

function getPath(iNode) {
  return sMainDir + iNode.slice(0, 4).toLowerCase() +'/'+ iNode.toLowerCase();
}

function getSub(iNode) {
  return sMainDir + iNode.slice(0, 4).toLowerCase();
}

function noop(err) { if (err) throw err; }

var sLock = {
  rsrc: {},

  set: function(iId, iFn) {
    if (!this.rsrc[iId]) {
      this.rsrc[iId] = {set:true};
      return true;
    }
    if (!this.rsrc[iId].set)
      return this.rsrc[iId].set = true;
    if (!this.rsrc[iId].queue) {
      this.rsrc[iId].queue = [];
      this.rsrc[iId].next = 0;
    }
    this.rsrc[iId].queue.push(iFn);
    return false;
  } ,

  free: function(iId) {
    if (this.rsrc[iId].queue && this.rsrc[iId].next < this.rsrc[iId].queue.length) {
      this.rsrc[iId].set = false;
      this.rsrc[iId].queue[this.rsrc[iId].next++]();
    } else {
      delete this.rsrc[iId];
    }
  }
};

function _sendNext(iNode, iN) {
  if (!(iNode in sActive))
    return;
  if (iN === undefined) {
    for (iN=0; iN < sQueues[iNode].length && !sQueues[iNode][iN]; ++iN) {}
    if (iN === sQueues[iNode].length)
      return;
  }
  ++sQueues[iNode].tries;
  if (!sQueues[iNode][iN].msg)
    fs.readFile(getPath(iNode)+'/'+sQueues[iNode][iN].id, function(err, data) {
      if (err) throw err;
      sQueues[iNode].size += data.length;
      sQueues[iNode][iN].msg = data;
      if (!(iNode in sActive)) {
        --sQueues[iNode].tries;
        return;
      }
      sActive[iNode].conn.send(data);
    });
  else
    sActive[iNode].conn.send(sQueues[iNode][iN].msg);
  sQueues[iNode].timer = setTimeout(_sendNext, 60*1000, iNode, iN);
}

function _newQueue(iNode) {
  var aQ = [];
  aQ.timer = null;
  aQ.tries = 0;
  aQ.lastDelivery = 0;
  aQ.size = 0;
  aQ.quiet = null;
  sQueues[iNode] = aQ;
}

function startQueue(iNode) {
  if (sQueues[iNode]) {
    sQuiet.remove(sQueues[iNode].quiet);
    sQueues[iNode].quiet = null;
    if (typeof sQueues[iNode].tries === 'number')
      _sendNext(iNode);
    return;
  }
  if (!sLock.set(iNode, function(){startQueue(iNode)} ))
    return;
  var aQ = sQueues[iNode] = { };
  fs.readdir(getPath(iNode), function(err, array) {
    if (err && err.errno !== process.ENOENT) throw err;
    _newQueue(iNode);
    if (err)
      return;
    array.sort();
    for (var a=0; a < array.length; ++a)
      sQueues[iNode].push({id:array[a]});
    if (aQ.quiet)
      sQueues[iNode].quiet = aQ.quiet;
    else
      _sendNext(iNode);
    sLock.free(iNode);
  });
}

function stopQueue(iNode) {
  if (sQueues[iNode].timer) {
    clearTimeout(sQueues[iNode].timer);
    sQueues[iNode].timer = null;
  }
  sQueues[iNode].quiet = sQuiet.append(iNode);
}

function queueItem(iNode, iId, iMsg, iCallback) {
  if (!sLock.set(iNode, function(){queueItem(iNode, iId, iMsg, iCallback)}))
    return;
  fs.mkdir(getSub(iNode), 0700, function(errSub) {
    if (errSub && errSub.errno !== process.EEXIST) throw errSub;
    fs.mkdir(getPath(iNode), 0700, function(errNode) {
      if (errNode && errNode.errno !== process.EEXIST) throw errNode;
      fs.link(sTempDir+iId, getPath(iNode)+'/'+iId, function(err) {
        if (err) throw err;
        if (iNode in sQueues) {
          sQueues[iNode].push({id:iId, msg:iMsg});
          sQueues[iNode].size += iMsg.length;
          if (sQueues[iNode].tries === 0)
            _sendNext(iNode);
        }
        sLock.free(iNode);
        var aDone = function(err) {
          if (err) throw err;
          if (!errSub) errSub = true;
          else if (!errNode) errNode = true;
          else iCallback();
        };
        if (!errSub) syncFile(sMainDir, aDone);
        if (!errNode) syncFile(getSub(iNode), aDone);
        syncFile(getPath(iNode), aDone);
      });
    });
  });
}

function deQueueItem(iNode, iId) {
  if (!(iNode in sQueues))
    return;
  for (var aN=0; aN < sQueues[iNode].length && (!sQueues[iNode][aN] || sQueues[iNode][aN].id < iId); ++aN) {}
  if (iId < 0 || aN === sQueues[iNode].length)
    throw 'ack id out of range';
  if (sQueues[iNode][aN].id > iId || !sQueues[iNode][aN])
    return;
  fs.unlink(getPath(iNode)+'/'+iId, noop);
  sQueues[iNode].size -= sQueues[iNode][aN].msg.length;
  sQueues[iNode][aN] = null;
  sQueues[iNode].tries = 0;
  sQueues[iNode].lastDelivery = Date.now();
  if (sQueues[iNode].timer) {
    clearTimeout(sQueues[iNode].timer);
    sQueues[iNode].timer = null;
  }
  if (aN+1 < sQueues[iNode].length)
    _sendNext(iNode, aN+1);
  else
    sQueues[iNode].length = 0;
}

// Linked list of inactive queues
var sQuiet = {
  head: null,
  tail: null,
  size: 0,

  append: function(iNode) {
    this.size += sQueues[iNode].size;
    if (this.size > sMaxQuietSize)
      process.nextTick(function() { sQuiet.clean() });
    var aI = { prev:this.tail, next:null, node:iNode };
    if (this.tail)
      this.tail = this.tail.next = aI;
    else
      this.head = this.tail = aI;
    return aI;
  } ,

  remove: function(iItem) {
    this.size -= sQueues[iItem.node].size;
    if (iItem.prev)
      iItem.prev.next = iItem.next;
    if (iItem.next)
      iItem.next.prev = iItem.prev;
    if (iItem === this.head)
      this.head = iItem.next;
    if (iItem === this.tail)
      this.tail = iItem.prev;
  } ,

  clean: function() {
    while (this.head && this.size >= sMaxQuietSize) {
      if (sQueues[this.head.node].length === 0)
        fs.rmdir(getPath(this.head.node), noop);
      this.size -= sQueues[this.head.node].size;
      delete sQueues[this.head.node];
      this.head = this.head.next;
      if (this.head)
        this.head.prev = null;
    }
  }
}; // sQuiet

// Connection handler
function Link(iConn) {
  this.loginTimer = setTimeout(function(link) { link.timeout(); }, 2000, this);
  this.conn = iConn;
  this.node = null;
}

Link.prototype = {

  params: {
    register: { nodeid:'string' },
    login:    { nodeid:'string' },
    post:     { to:'object', id:'string' },
    ack:      { type:'string', id:'string' }
  } ,

  handleMessage: function(iMsg) {
    var aJsEnd = parseInt(iMsg.toString('ascii', 0, 4), 16) +4;
    if (aJsEnd === NaN)
      throw 'invalid length header';

    try {
    var aReq = JSON.parse(iMsg.toString('ascii', 4, aJsEnd));
    } catch (err) {
      throw 'invalid json header';
    }

    if (typeof aReq.op !== 'string' || typeof this.params[aReq.op] === 'undefined')
      throw 'invalid request op';

    for (var a in this.params[aReq.op]) {
      if (typeof aReq[a] !== this.params[aReq.op][a])
        throw 'missing request param '+a;
    }

    var aBuf = iMsg.length > aJsEnd ? iMsg.slice(aJsEnd, iMsg.length) : null;

    this[aReq.op](aReq, aBuf);
  } ,

  timeout: function() {
    this.loginTimer = null;
    this.conn.send(makeMsg({op:'quit', info:'close timeout'}));
    this.conn.close();
  } ,

  register: function(iReq) {
    this._activate(iReq.nodeid, 'ok register');
  } ,

  login: function(iReq) {
    var that = this;
    process.nextTick(function(err) { // validate
      if (!that.conn)
        return;
      if (err) {
        that.conn.send(makeMsg({op:'quit', info:'invalid login'}));
        that.conn.close();
        return;
      }
      that._activate(iReq.nodeid, 'ok login');
      startQueue(iReq.nodeid);
    });
  } ,

  _activate: function(iNode, iAck) {
    clearTimeout(this.loginTimer);
    this.loginTimer = null;
    this.node = iNode;
    sActive[iNode] = this;
    this.conn.send(makeMsg({op:'info', info:iAck}));
    if (sShutdown)
      this.conn.close();
  } ,

  sLastId: 0,
  sLastSubId: 0,
  _makeId: function() {
    var aId = Date.now();
    if (aId === Link.prototype.sLastId)
      return aId +'-'+ ++Link.prototype.sLastSubId;
    Link.prototype.sLastId = aId;
    Link.prototype.sLastSubId = 0;
    return aId.toString();
  } ,

  post: function(iReq, iBuf) {
    if (!this.node)
      throw 'illegal op on unauthenticated socket';
    var aName = false;
    for (aName in iReq.to) break;
    if (!aName)
      throw 'missing to members';
    var that = this;
    var aFail = function() {
      if (that.conn)
        that.conn.send(makeMsg({op:'ack', type:'fail', id:iReq.id}));
    };
    var aId = this._makeId();
    var aMsg = makeMsg({op:'deliver', id:aId, from:that.node}, iBuf);
    fs.open(sTempDir+aId, 'w', 0600, function(err, fd) {
      if (err) return aFail();
      writeAll(fd, aMsg, function(err) { // attempt write to temp
        if (err) { fs.close(fd); return aFail(); }
        fs.fsync(fd, function(err) {
          fs.close(fd);
          if (err) return aFail();
          var aToCount = 0;
          var aCb = function() {
            if (--aToCount > 0)
              return;
            if (that.conn)
              that.conn.send(makeMsg({op:'ack', type:'ok', id:iReq.id}));
            fs.unlink(sTempDir+aId, noop);
          };
          for (var a in iReq.to) {
            ++aToCount;
            queueItem(a, aId, aMsg, aCb);
          }
        });
      });
    });
  } ,

  ack: function(iReq) {
    if (!this.node)
      throw 'illegal op on unauthenticated socket';
    if (iReq.type === 'ok')
      deQueueItem(this.node, iReq.id);
  } ,

  finalize: function() {
    if (this.node) {
      stopQueue(this.node);
      delete sActive[this.node];
    }
    if (this.loginTimer)
      clearTimeout(this.loginTimer);
    this.conn = null;
  }
};

main(process.argv);

function test() {
  sToList = { aabba:true, bbccb:true, ccddc:true, ddeed:true, eeffe:true, ffggf:true, gghhg:true, hhiih:true, iijji:true, jjkkj:true };
  sMsgList = [ 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten' ];
  for (var a=0; a < sMsgList.length; ++a)
    sMsgList[a] = new Buffer(sMsgList[a]);

  function Testconn(iId) {
    this.link = null;
    this.id = iId;
    this.open = false;
    this.recv = {};
  }

  Testconn.prototype = {
    send: function(iMsg) {
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
              that.link.handleMessage(makeMsg({op:'ack', type:'ok', id:aReq.id}));
          }, aT*10);
          if (aBuf in that.recv)
            ++that.recv[aBuf];
          else
            that.recv[aBuf] = 1;
          if (that.recv[aBuf] % 10 === 0)
            console.log(that.id+' got 10 '+aBuf);
        } else if (aReq.op === 'ack') {
          that.ack[+aReq.id] = true;
        } else
          console.log(sys.inspect(aReq));
      } else
        console.log(iMsg);
    } ,

    connect: function() {
      this.open = true;
      this.link = new Link(this);
      this.ack = [];
      this.ack.length = sMsgList.length;
    } ,

    close: function() {
      for (var a=0, aTot=0; a < this.ack.length; ++a)
        if (this.ack[a]) ++aTot;
      console.log(this.id+' '+aTot+' ackd');
      this.open = false;
      this.link.finalize();
      this.link = null;
    }
  }

  function testLink(aC, iState) {
    switch (iState) {
    case 0:
      if (sShutdown)
        break;
      aC.connect();
      aC.link.handleMessage(makeMsg({op:'login', nodeid:aC.id}));
      setTimeout(testLink, (Date.now()%10)*1000, aC, iState+1);
      break;
    case 1: case 2: case 3: case 4: case 5: case 6: case 7: case 8: case 9: case 10:
      if (!aC.link)
        break;
      var aMsg = makeMsg({op:'post', to:sToList, id:(iState-1).toString()}, sMsgList[iState-1]);
      aC.link.handleMessage(aMsg);
      setTimeout(testLink, (Date.now()%10)*800, aC, iState+1);
      break;
    case 11:
      if (sShutdown)
        break;
      aC.close();
      setTimeout(testLink, (Date.now()%10)*800, aC, 0);
      break;
    }
  }

  for (var a in sToList) {
    testLink(new Testconn(a), 0);
  }
}

