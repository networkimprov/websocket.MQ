
var sys = require('sys');
var fs = require('fs');

var ws = require('./websocket-server/lib/ws/server');

var sMainDir = 'mqstore/';
var sTempDir = sMainDir+'temp/';
var sMsgCacheMax = 100000;
var sQuietHoursMax = 28;
var sQuietCleanPeriod = 20*1000;

var sQueues = {}; // array objects indexed by nodeid
var sActive = {}; // connections indexed by nodeid
var sShutdown = false;

function main(argv) {
  try {
  fs.mkdirSync(sMainDir, 0700);
  } catch (err) {
    if (err.errno !== process.EEXIST) throw err;
  }
  try {
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
        console.log(err.message);
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
    conn.addListener("timeout", function() {
      conn.send(makeMsg({op:'quit', info:'timeout'}));
      conn.close();
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

  read:  function(iId, iFn) { return this._lock(iId, iFn, 'read', 'write') } ,
  write: function(iId, iFn) { return this._lock(iId, iFn, 'write', 'read') } ,

  _lock: function(iId, iFn, iA, iB) {
    if (!this.rsrc[iId])
      this.rsrc[iId] = {};
    if (!this.rsrc[iId][iB]) {
      if (!this.rsrc[iId][iA])
        this.rsrc[iId][iA] = 0;
      ++this.rsrc[iId][iA];
      return true;
    }
    if (!this.rsrc[iId].queue)
      this.rsrc[iId].queue = [];
    this.rsrc[iId].queue.push(iFn);
    return false;
  } ,

  free: function(iId) {
    var aType = this.rsrc[iId].read ? 'read' : 'write';
    if (--this.rsrc[iId][aType] > 0)
      return;
    if (this.rsrc[iId].queue) {
      for (var a=0; a < this.rsrc[iId].queue.length; ++a)
        this.rsrc[iId].queue[a]();
      delete this.rsrc[iId].queue;
    } else {
      delete this.rsrc[iId];
    }
  }
};

function _sendNext(iNode) {
  if (!(iNode in sActive) || sQueues[iNode].length === 0)
    return;
  ++sQueues[iNode].tries;
  var aN = sQueues[iNode].next;
  if (!sQueues[iNode][aN]) sys.debug(sys.inspect(sQueues[iNode])+' queue '+iNode+' n '+aN+' len '+sQueues[iNode].length);
  var aLn = sActive[iNode];
  var aId = sQueues[iNode][aN];
  sMsgCache.get(iNode, aId, function(msg) {
    if (!msg && sQueues[iNode][aN] === aId) throw new Error('null msg for '+iNode+' '+aId);
    if (sActive[iNode] === aLn && sQueues[iNode][aN] === aId)
      sActive[iNode].conn.send(msg, function(type) {
        if (sActive[iNode] === aLn && sQueues[iNode][aN] === aId)
          sQueues[iNode].timer = setTimeout(_sendNext, 10*1000, iNode);
      });
  });
}

function _newQueue(iNode, ioArray) {
  if ('tries' in sQueues[iNode])
    throw new Error('queue already exists');
  ioArray.sort();
  for (var a=0; a < ioArray.length; ++a)
    sMsgCache.link(ioArray[a]);
  ioArray.timer = null;
  ioArray.tries = 0;
  ioArray.next = 0;
  ioArray.quiet = null;
  sQueues[iNode] = ioArray;
}

function _deleteQueue(iNode) {
  if (sQueues[iNode].timer)
    throw new Error('delete of active queue');
  if (sQueues[iNode].length === 0)
    fs.rmdir(getPath(iNode), noop);
  for (var a=0; a < sQueues[iNode].length; ++a)
    if (sQueues[iNode][a])
      sMsgCache.unlink(sQueues[iNode][a]);
  delete sQueues[iNode];
}

function startQueue(iNode) {
  if (sQueues[iNode]) {
    sQuiet.remove(sQueues[iNode].quiet);
    sQueues[iNode].quiet = null;
    if (typeof sQueues[iNode].tries === 'number')
      _sendNext(iNode);
    return;
  }
  if (!sLock.read(iNode, function(){startQueue(iNode)} ))
    return;
  var aQ = sQueues[iNode] = { };
  fs.readdir(getPath(iNode), function(err, array) {
    if (err && err.errno !== process.ENOENT) throw err;
    _newQueue(iNode, array || []);
    if (aQ.quiet)
      sQueues[iNode].quiet = aQ.quiet;
    else if (sQueues[iNode].length)
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

function queueItem(iNode, iId, iCallback) {
  if (!sLock.write(iNode, function(){queueItem(iNode, iId, iCallback)}))
    return;
  fs.mkdir(getSub(iNode), 0700, function(errSub) {
    if (errSub && errSub.errno !== process.EEXIST) throw errSub;
    fs.mkdir(getPath(iNode), 0700, function(errNode) {
      if (errNode && errNode.errno !== process.EEXIST) throw errNode;
      fs.link(sTempDir+iId, getPath(iNode)+'/'+iId, function(err) {
        if (err) throw err;
        if (iNode in sQueues) {
          sQueues[iNode].push(iId);
          sMsgCache.link(iId);
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
  if (sQueues[iNode].length === 0 || sQueues[iNode][sQueues[iNode].next] !== iId)
    return;
  fs.unlink(getPath(iNode)+'/'+iId, noop);
  sMsgCache.unlink(iId);
  sQueues[iNode][sQueues[iNode].next] = null;
  sQueues[iNode].tries = 0;
  if (sQueues[iNode].timer) {
    clearTimeout(sQueues[iNode].timer);
    sQueues[iNode].timer = null;
  }
  if (++sQueues[iNode].next < sQueues[iNode].length)
    _sendNext(iNode);
  else
    sQueues[iNode].next = sQueues[iNode].length = 0;
}

function LList() {
  this.head = null;
  this.tail = null;
}

LList.prototype = {
  append: function(iObj) {
    iObj._prev = this.tail;
    iObj._next = null;
    if (this.tail)
      this.tail = this.tail._next = iObj;
    else
      this.head = this.tail = iObj;
  } ,

  remove: function(iItem) {
    if (iItem._prev)
      iItem._prev._next = iItem._next;
    if (iItem._next)
      iItem._next._prev = iItem._prev;
    if (iItem === this.head)
      this.head = iItem._next;
    if (iItem === this.tail)
      this.tail = iItem._prev;
    delete iItem._prev;
    delete iItem._next;
  }
}

// Linked list of inactive queues
var sQuiet = {
  list: new LList(),
  timer: null,

  append: function(iNode) {
    var aI = { node:iNode, lastOn:Date.now() };
    this.list.append(aI);
    if (!this.timer)
      this.timer = setTimeout(function(){sQuiet.clean()}, sQuietCleanPeriod);
    return aI;
  } ,

  remove: function(iItem) {
    this.list.remove(iItem);
    if (!this.list.head) {
      clearTimeout(this.timer);
      this.timer = null;
    }
  } ,

  clean: function() {
    var aCutoff = Date.now() - 15*1000; /// sQuietHoursMax * 60*60*1000;
    while (this.list.head && this.list.head.lastOn < aCutoff) {
      _deleteQueue(this.list.head.node);
      this.list.remove(this.list.head);
    }
    this.timer = setTimeout(function(){sQuiet.clean()}, sQuietCleanPeriod);
  }
}; // sQuiet

var sMsgCache = {
  cache: {}, // indexed by file id
  list: new LList(), // ordered by add order
  size: 0,

  get: function(iNode, iId, iCallback) {if (!this.cache[iId]) sys.debug('cache id '+iId+' node '+iNode);
    if (this.cache[iId].msg) {
      process.nextTick(function() { iCallback(sMsgCache.cache[iId].msg) });
      return;
    }
    if (this.cache[iId].wait) {
      this.cache[iId].wait[iNode] = iCallback;
      return;
    }
    this.cache[iId].wait = {};
    this.cache[iId].wait[iNode] = iCallback;
    var aWait = this.cache[iId].wait;
    function aRead(queue) {
      fs.readFile(getPath(queue)+'/'+iId, function(err, data) {
        if (err && err.errno !== process.ENOENT) throw err;
        if (!(iId in sMsgCache.cache)) {
          for (var a in aWait)
            aWait[a](null);
          return;
        }
        if (err) {
          aWait[queue](null);
          delete aWait[queue];
          for (var a in aWait)
            return aRead(a);
        } else {
          sMsgCache.put(iId, data);
          for (var a in aWait)
            aWait[a](data);
        }
        delete sMsgCache.cache[iId].wait;
      });
    }
    aRead(iNode);
  } ,

  put: function(iId, iMsg) {
    if (iMsg.length > sMsgCacheMax/10)
      return;
    if (iId in this.cache)
      this.cache[iId].msg = iMsg;
    else
      this.cache[iId] = { count:0, msg:iMsg };
    this.list.append(this.cache[iId]);
    this.size += iMsg.length;
    if (this.size > sMsgCacheMax)
      process.nextTick(function() { sMsgCache.clean() });
  } ,

  link: function(iId) {
    if (iId in this.cache)
      ++this.cache[iId].count;
    else
      this.cache[iId] = { count:1, msg:null };
  } ,

  unlink: function(iId) {
    if (--this.cache[iId].count > 0)
      return;
    if (this.cache[iId].msg) {
      this.size -= this.cache[iId].msg.length;
      this.list.remove(this.cache[iId]);
    }
    delete this.cache[iId];
  } ,

  clean: function() {
    while (this.list.head && this.size > sMsgCacheMax) {
      this.size -= this.list.head.msg.length;
      this.list.head.msg = null;
      this.list.remove(this.list.head);
    }
  }
};

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

    if (!this.conn)
      throw 'message arrived on closed connection: '+JSON.stringify(iReq);

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
    });
  } ,

  _activate: function(iNode, iAck) {
    if (iNode in sActive || sShutdown) {
      this.conn.send(makeMsg({op:'quit', info:(sShutdown ? 'shutdown' : 'login already active')}));
      this.conn.close();
      return;
    }
    clearTimeout(this.loginTimer);
    this.loginTimer = null;
    this.node = iNode;
    sActive[iNode] = this;
    this.conn.send(makeMsg({op:'info', info:iAck}));
    startQueue(iNode);
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
          sMsgCache.put(aId, aMsg);
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
            queueItem(a, aId, aCb);
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
    if (!this.conn) {
      console.log('finalize called on finalized Link');
      return;
    }
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
  sToList = {
    aabba:true, bbccb:true, ccddc:true, ddeed:true, eeffe:true, ffggf:true, gghhg:true, hhiih:true, iijji:true, jjkkj:true,
    abcde:true, bcdef:true, cdefg:true, defgh:true, efghi:true, fghij:true, ghijk:true, hijlk:true, ijlkm:true, jklmn:true
  };
  sMsgList = [ 'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten' ];
  for (var a=0; a < sMsgList.length; ++a)
    sMsgList[a] = new Buffer(sMsgList[a]);

  function Testconn(iId) {
    this.link = null;
    this.id = iId;
    this.open = false;
    this.recv = {};
    this.ack = [];
    this.ack.length = sMsgList.length;
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
          }, aT*5);
          if (aBuf in that.recv)
            ++that.recv[aBuf];
          else
            that.recv[aBuf] = 1;
          if (that.recv[aBuf] % 20 === 0)
            console.log(that.id+' got 20 '+aBuf);
        } else if (aReq.op === 'ack') {
          ++that.ack[+aReq.id];
        } else
          console.log(sys.inspect(aReq));
      } else
        console.log(iMsg);
    } ,

    connect: function() {
      this.open = true;
      this.link = new Link(this);
      for (var a=0; a < this.ack.length; ++a)
        this.ack[a] = 0;
    } ,

    close: function() {
      var aList = '';
      for (var a=0; a < this.ack.length; ++a)
        aList += ' '+a+':'+this.ack[a];
      console.log(this.id+aList+' ackd');
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
      setTimeout(testLink, (Date.now()%10)*907, aC, iState+1);
      break;
    case  1: case  2: case  3: case  4: case  5: case  6: case  7: case  8: case  9: case 10:
    case 11: case 12: case 13: case 14: case 15: case 16: case 17: case 18: case 19: case 20:
      if (!aC.link)
        break;
      var aTo = {}, aN = Date.now()%20+1;
      for (var a in sToList) { aTo[a] = sToList[a]; if (--aN === 0) break; }
      var aMsg = makeMsg({op:'post', to:sToList, id:(iState%10).toString()}, sMsgList[iState%10]);
      aC.link.handleMessage(aMsg);
      setTimeout(testLink, (Date.now()%10)*807, aC, iState+1);
      break;
    case 21:
      if (sShutdown)
        break;
      aC.close();
      setTimeout(testLink, (Date.now()%30)*1007, aC, 0);
      break;
    }
  }

  for (var a in sToList) {
    setTimeout(function(a){testLink(new Testconn(a), 0)}, 0, a);
  }
}

