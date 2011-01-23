
var net = require('net');
var sys = require('sys');
var WsStream = require('./wsstream');

var sSide = process.argv.length > 2 ? 'client' : 'server';
if (sSide === 'client')
  runClient()
else
  runServer();

function checkMsg(iFrame, iMsg, iCount) {
  if (iFrame.opcode !== 'binary' || !iFrame.isFinal || iFrame.size !== iMsg.length)
    throw new Error('bad frame '+sys.inspect(iFrame));
  var aTail = iMsg.toString('ascii', iMsg.length-11);
  if (aTail !== ' badgirl!  ')
    throw new Error('msg corrupted: '+aTail);
  if (iCount % 1000 === 0)
    console.log(sSide+' '+iCount+' m '+iMsg.length);
}

function handleErr(iErr) {
  switch(iErr.errno) {
  case process.ENOTCONN:
  case process.ECONNRESET:
  case process.EPIPE:
  case process.EAGAIN:
  case process.ECONNREFUSED:
    console.log(iErr.message+' '+sSide);
    break;
  default:
    throw iErr;
  }
}

function runServer() {
  var aClosing = false;
  var aServer = net.createServer(function(socket) {
    socket.setNoDelay(true);

    socket.on('timeout', function() {
      ;
    });

    socket.on('close', function(err) {
      console.log('server close');
    });

    socket.on('error', handleErr);

    var aWs = new WsStream(socket);

    var aCount = 0;
    aWs.on('data', function(frame, msg) {
      checkMsg(frame, msg, ++aCount);
      if (aClosing && socket.writable) {
        aWs.end();
        return;
      }
      var aBuf = new Buffer(msg.length+11);
      msg.copy(aBuf, 0, 0, msg.length);
      aBuf.write(' badgirl!  ', msg.length);
      aWs.write(1, 'binary', aBuf);
    });

    aWs.on('end', function(ok) {
      console.log('server got end '+ok);
    });
  });

  process.on('SIGINT', function() {
    aClosing = true;
    aServer.close();
  });

  aServer.listen(8009, function() {
    var aC = require('child_process').spawn(process.argv[0], [process.argv[1], 'client'], {customFds:[-1, process.stdout.fd, process.stdout.fd]});
  });
}


function runClient() {
  var aStr = 'here is the test string for starters. badgirl!  ';
  //var sBuf = new Buffer(16383);
  //for (var a=0; a < sBuf.length; a+=aStr.length)
  //  sBuf.write(aStr, a);
  //var sStr = sBuf.toString('ascii');

  var aClient = new net.Stream;
  aClient.on('connect', function() {
    console.log('client connected');
    aClient.setNoDelay(true);
    aWsc.write(1, 'binary', aStr);
  });

  aClient.on('timeout', function() {
    ;
  });

  aClient.on('close', function(err) {
    console.log('client close');
  });

  aClient.on('error', handleErr);

  var aWsc = new WsStream(aClient);

  var aCount = 0;
  aWsc.on('data', function(frame, msg) {
    checkMsg(frame, msg, ++aCount);
    aWsc.write(1, 'binary', msg);
    if (aCount % 1727 === 0)
      aWsc.write(1, 'binary', msg, function(ok) {
        console.log('callback '+ok);
      });
  });

  aWsc.on('end', function(ok) {
    console.log('client got end '+ok);
  });

  process.on('SIGINT', function(){});

  aClient.connect(8009);
}

