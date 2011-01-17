
// Websocket draft-04 compatible Stream class
// supports callbacks to write
// todo:
//   allow write of a Stream
//   Websocket handshaking

module.exports = WsStream;

function WsStream(iSocket) {
  this.socket = iSocket;
  this.framebuf = new Buffer(10);
  this.framelen = 0;
  this.frame = null;
  this.inBufs = [];
  this.inLen = 0;

  var that = this;

  this.socket.on('data', function(data) {
    that.inBufs.push(data);
    that.inLen += data.length;
    if (!that.frame || that.inLen >= that.frame.size)
      that._read();
  });

  this.socket.on('end', function() {
    if (that.inBufs.length)
      throw new Error('dangling buffers after "end" event');
    if (that.event_end)
      that.event_end();
    //if (!that.socket.writable && that.event_close)
    //  that.event_close(false);
  });
}

var sOpCode  = {  cont:0, close:1, ping:2, pong:3, text:4, binary:5 };
var sOpCodeA = [ 'cont', 'close', 'ping', 'pong', 'text', 'binary'  ];

WsStream.prototype = {

  write: function(iFinal, iOp, iBuf, iCallback) {
    if (!(iOp in sOpCode))
      throw new Error('invalid opcode: '+iOp);
    if (!this.socket.writable) {
      if (iCallback)
        process.nextTick(function() { iCallback(false) });
      return;
    }

    var aLen = typeof iBuf === 'string' ? Buffer.byteLength(iBuf) : iBuf.length;
    var aFlen = aLen < 0x7E ? 2 : aLen <= 0xFFFF ? 4 : 10;

    var aBuf = new Buffer(aLen+aFlen);
    aBuf[0] = (iFinal ? 0x80 : 0) | sOpCode[iOp];

    if (aLen < 0x7E) {
      aBuf[1] = aLen;
    } else if (aLen <= 0xFFFF) {
      aBuf[1] = 0x7E;
      aBuf[2] = aLen >> 8;
      aBuf[3] = aLen & 0xFF;
    } else {
      aBuf[1] = 0x7F;
      for (var aB=0; aB < 8; ++aB)
        aBuf[2+aB] = aLen >> 8*(7-aB) & 0xFF;
    }
    if (typeof iBuf === 'string')
      aBuf.write(iBuf, aFlen);
    else
      iBuf.copy(aBuf, aFlen, 0, iBuf.length);

    var aNow = this.socket.write(aBuf);
    if (iCallback) {
      if (aNow) {
        process.nextTick(function() { iCallback(true) });
      } else {
        var that = this;
        that.socket.on("drain", function f() { // should use once("drain", callback)
          that.socket.removeListener("drain", f);
          iCallback(true);
        });
      }
    }
  } ,

  _read: function() {
    if (!this.frame) {
      var aLen = this.framelen >= 2 ? this.framebuf[1] & 0x7F : -1;
      for (var a=0; a < this.inBufs[0].length; ++a) {
        this.framebuf[this.framelen++] = this.inBufs[0][a];
        if (this.framelen === 2)
          aLen = this.framebuf[1] & 0x7F;
        if (this.framelen === 2 && aLen <   0x7E ||
            this.framelen === 4 && aLen === 0x7E ||
            this.framelen === 10)
          break;
      }
      if (a < this.inBufs[0].length) {
        ++a;
        this.framelen = 0;
        this.frame = { isFinal: this.framebuf[0] & 0x80, opcode: sOpCodeA[this.framebuf[0] & 0x0F], size: 0 };
        switch (aLen) {
        case 0x7E:
          this.frame.size = this.framebuf[2] << 8 | this.framebuf[3];
          break;
        case 0x7F:
          for (var aB=0; aB < 8; ++aB)
            this.frame.size = this.frame.size << 8 | this.framebuf[2+aB];
          break;
        default:
          this.frame.size = aLen;
        }
      }
      this.inLen -= a;
      if (a === this.inBufs[0].length)
        this.inBufs.shift();
      else
        this.inBufs[0] = this.inBufs[0].slice(a, this.inBufs[0].length);
      if (this.inBufs.length && (!this.frame || this.inLen >= this.frame.size))
        this._read();
      return;
    }
    var aBuf;
    if (this.frame.size === this.inBufs[0].length) {
      aBuf = this.inBufs.shift();
    } else {
      aBuf = new Buffer(this.frame.size);
      for (var aSeg, aLen=0; aLen < this.frame.size; aLen += aSeg) {
        aSeg = Math.min(this.frame.size-aLen, this.inBufs[0].length);
        this.inBufs[0].copy(aBuf, aLen, 0, aSeg);
        if (aSeg === this.inBufs[0].length)
          this.inBufs.shift();
        else
          this.inBufs[0] = this.inBufs[0].slice(aSeg, this.inBufs[0].length);
      }
    }
    this.inLen -= this.frame.size;
    if (this.event_data)
      this.event_data(this.frame, aBuf);
    this.frame = null;
    if (this.inBufs.length)
      this._read();
  } ,

  end: function() {
    this.socket.end();
    //if (!this.socket.readable && this.event_close)
    //  this.event_close(false);
  } ,

  on: function(iEvt, iFn) {
    if (typeof iEvt !== 'string')
      throw new Error('not a string '+iEvt);
    if (typeof iFn !== 'function')
      throw new Error('not a function '+iFn);
    this['event_'+iEvt] = iFn;
  }

};

