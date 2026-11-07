import http from 'node:http';
import { randomBytes } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { WebSocket, WebSocketServer } from 'ws';

export const PROTOCOL = 'ashrift-party-1';
const AUTHORITY_EVENTS = new Set(['snapshot','effect','finished','outfits','profiles','custom_effect','coop_state','coop_fx']);
const CLIENT_EVENTS = new Set(['submit','register_profile','register_loadout']);
export function createPartyServer({maxRooms=100, maxConnections=600, timeoutMs=30000}={}) {
  const rooms=new Map(), sockets=new Set(), attempts=new Map();
  const server=http.createServer((req,res)=>{
    res.writeHead(req.url==='/health'?200:404,{'content-type':'application/json','cache-control':'no-store'});
    res.end(JSON.stringify(req.url==='/health'?{ok:true,protocol:PROTOCOL,rooms:rooms.size}: {error:'Not found'}));
  });
  const ws=new WebSocketServer({server,path:'/party',maxPayload:512*1024,perMessageDeflate:false});
  function send(socket,value) {
    if(socket.readyState!==WebSocket.OPEN)return;
    if(socket.bufferedAmount>2*1024*1024){socket.close(1013,'Slow connection');return;}
    socket.send(JSON.stringify(value));
  }
  function fail(socket,error) { send(socket,{type:'error',error});socket.close(1008,'Invalid room request'); }
  function dispose(socket) {
    sockets.delete(socket);
    const room=socket.room;
    if(!room)return;
    socket.room=null;
    room.peers.delete(socket.peerId);
    if(socket.peerId===1) {
      rooms.delete(room.code);
      for(const peer of room.peers.values()) {
        peer.room=null;
        send(peer,{type:'host_left'});
        peer.close(1000,'Host left');
      }
      room.peers.clear();
    } else {
      for(const peer of room.peers.values())send(peer,{type:'peer_left',id:socket.peerId});
    }
  }
  ws.on('connection',(socket,request)=>{
    if(sockets.size>=maxConnections){socket.close(1013,'Server full');return;}
    const ip=request.socket.remoteAddress;
    const now=Date.now(), rate=attempts.get(ip)||{start:now,count:0};
    if(now-rate.start>60000){rate.start=now;rate.count=0;}
    if(++rate.count>60){socket.close(1013,'Connection limit');return;}
    attempts.set(ip,rate);
    sockets.add(socket);socket.alive=true;socket.rateAt=now;socket.messageCount=0;socket.connectedAt=now;
    socket.on('pong',()=>{socket.alive=true;});
    socket.on('error',()=>dispose(socket));
    socket.on('close',()=>dispose(socket));
    socket.on('message',data=>{
      if(Date.now()-socket.rateAt>1000){socket.rateAt=Date.now();socket.messageCount=0;}
      if(++socket.messageCount>180){socket.close(1008,'Message limit');return;}
      let message;
      try{message=JSON.parse(data.toString());}catch{return fail(socket,'訊息格式錯誤。');}
      if(!message||typeof message!=='object')return fail(socket,'訊息格式錯誤。');
      if(message.type==='enter') {
        if(socket.room)return fail(socket,'已加入房間。');
        if(message.version!==PROTOCOL)return fail(socket,'遊戲版本不相容，請更新至相同版本。');
        let room;
        if(message.action==='create') {
          if(rooms.size>=maxRooms)return fail(socket,'房間已滿，請稍後再試。');
          if(!['pvp','coop'].includes(message.mode))return fail(socket,'模式無效。');
          let code;
          do{code=randomBytes(4).toString('hex').toUpperCase();}while(rooms.has(code));
          room={code,mode:message.mode,peers:new Map(),nextId:2,started:false};
          rooms.set(code,room);socket.peerId=1;
        } else if(message.action==='join') {
          const code=String(message.code||'').trim().toUpperCase();
          room=rooms.get(code);
          if(!room)return fail(socket,'找不到房間，請確認房號及伺服器網址。');
          if(room.started)return fail(socket,'此房間已開始，請等待房主重新開房。');
          if(room.peers.size>=6)return fail(socket,'房間已滿（最多6人）。');
          socket.peerId=room.nextId++;
        } else return fail(socket,'房間操作無效。');
        socket.room=room;room.peers.set(socket.peerId,socket);
        send(socket,{type:'welcome',id:socket.peerId,code:room.code,mode:room.mode,peers:[...room.peers.keys()]});
        for(const peer of room.peers.values())if(peer!==socket)send(peer,{type:'peer_joined',id:socket.peerId});
        return;
      }
      const room=socket.room;
      if(!room)return fail(socket,'請先加入房間。');
      if(message.type==='start'&&socket.peerId===1){room.started=true;return;}
      if(message.type!=='data'||typeof message.payload!=='string'||message.payload.length>480000||!/^[A-Za-z0-9+/]*={0,2}$/.test(message.payload))return;
      if(socket.peerId===1) {
        if(!AUTHORITY_EVENTS.has(message.event))return;
        const packet={type:'data',from:1,event:message.event,payload:message.payload};
        if(message.to===0) { for(const [id,peer] of room.peers)if(id!==1)send(peer,packet); }
        else if(Number.isInteger(message.to)&&message.to!==1) {
          const target=room.peers.get(message.to);
          if(target)send(target,packet);
        }
      } else if(message.to===1&&CLIENT_EVENTS.has(message.event)) {
        send(room.peers.get(1),{type:'data',from:socket.peerId,event:message.event,payload:message.payload});
      }
    });
  });
  const heartbeat=setInterval(()=>{
    for(const socket of sockets) {
      if(!socket.alive||(!socket.room&&Date.now()-socket.connectedAt>10000)){socket.terminate();dispose(socket);continue;}
      socket.alive=false;socket.ping();
    }
    for(const [ip,rate] of attempts)if(Date.now()-rate.start>60000)attempts.delete(ip);
  },timeoutMs);
  heartbeat.unref();
  server.on('close',()=>clearInterval(heartbeat));
  return {server,rooms,ws,async close(){for(const socket of sockets)socket.terminate();await new Promise(resolve=>ws.close(resolve));await new Promise(resolve=>server.close(resolve));}};
}
if(process.argv[1]&&import.meta.url===pathToFileURL(process.argv[1]).href) {
  const {server}=createPartyServer();
  server.listen(Number(process.env.PORT||8787),process.env.HOST||'0.0.0.0',()=>console.log('AshRift party relay ready'));
}

