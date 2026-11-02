const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;
const wss = new WebSocketServer({ port: PORT });

console.log(`[Gold Radio Server] Running on port ${PORT}`);

// تخزين الجلسات النشطة
// sessionId = hostId
const sessions = new Map();
const clientMeta = new Map(); // ws -> { userId, userName, currentSessionId }

function generatePin() {
    return Math.floor(10000 + Math.random() * 90000).toString();
}

function broadcastToSession(sessionId, message, excludeWs = null) {
    const session = sessions.get(sessionId);
    if (!session) return;
    const data = JSON.stringify(message);
    
    // إرسال للمضيف
    if (session.hostWs && session.hostWs.readyState === 1 && session.hostWs !== excludeWs) {
        session.hostWs.send(data);
    }
    // إرسال للمستمعين
    session.listeners.forEach(listener => {
        if (listener.ws && listener.ws.readyState === 1 && listener.ws !== excludeWs) {
            listener.ws.send(data);
        }
    });
}

function notifySessionMembersUpdate(sessionId) {
    const session = sessions.get(sessionId);
    if (!session) return;
    const members = [
        { id: session.hostId, name: session.hostName, isHost: true },
        ...session.listeners.map(l => ({ id: l.id, name: l.name, isHost: false }))
    ];
    broadcastToSession(sessionId, {
        type: "SESSION_MEMBERS",
        members: members
    });
}

wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    ws.on('message', (raw) => {
        try {
            const data = JSON.parse(raw);
            const { type } = data;

            // 1. تسجيل بيانات الاتصال المبدئية
            if (type === "REGISTER_CLIENT") {
                clientMeta.set(ws, { userId: data.userId, userName: data.userName, currentSessionId: null });
            }

            // 2. إنشاء جلسة استماع جديدة
            else if (type === "CREATE_SESSION") {
                const hostId = data.hostId;
                const hostName = data.hostName;
                const pin = generatePin();

                const session = {
                    sessionId: hostId,
                    pin: pin,
                    hostId: hostId,
                    hostName: hostName,
                    hostWs: ws,
                    track: null,
                    listeners: []
                };

                sessions.set(hostId, session);
                const meta = clientMeta.get(ws) || {};
                meta.currentSessionId = hostId;
                clientMeta.set(ws, meta);

                ws.send(JSON.stringify({
                    type: "SESSION_CREATED",
                    sessionId: hostId,
                    pin: pin
                }));
            }

            // 3. فحص اللاعبين المتواجدين في الغرفة (On-Demand Query)
            else if (type === "QUERY_ROOM_SESSIONS") {
                const roomUserNames = data.userNames || []; // قائمة الأسماء الموجودة بالروم
                const activeInRoom = [];

                sessions.forEach((session) => {
                    if (roomUserNames.includes(session.hostName)) {
                        activeInRoom.push({
                            hostId: session.hostId,
                            hostName: session.hostName,
                            pin: session.pin,
                            trackTitle: session.track ? session.track.title : "Ready"
                        });
                    }
                });

                ws.send(JSON.stringify({
                    type: "ROOM_SESSIONS_RESULT",
                    sessions: activeInRoom
                }));
            }

            // 4. إرسال طلب انضمام إلى المضيف
            else if (type === "REQUEST_JOIN") {
                const targetHostId = data.hostId;
                const guestId = data.guestId;
                const guestName = data.guestName;

                const session = sessions.get(targetHostId);
                if (session && session.hostWs && session.hostWs.readyState === 1) {
                    session.hostWs.send(JSON.stringify({
                        type: "JOIN_REQUEST_RECEIVED",
                        guestId: guestId,
                        guestName: guestName
                    }));
                } else {
                    ws.send(JSON.stringify({ type: "JOIN_ERROR", message: "Session no longer active" }));
                }
            }

            // 5. رد المضيف (قبول أو رفض)
            else if (type === "JOIN_RESPONSE") {
                const { hostId, guestId, accepted } = data;
                const session = sessions.get(hostId);
                if (!session) return;

                // البحث عن اتصال الضيف
                let guestWs = null;
                let guestName = "";
                for (let [clientSocket, meta] of clientMeta.entries()) {
                    if (meta.userId === guestId) {
                        guestWs = clientSocket;
                        guestName = meta.userName;
                        break;
                    }
                }

                if (guestWs && guestWs.readyState === 1) {
                    if (accepted) {
                        session.listeners.push({ id: guestId, name: guestName, ws: guestWs });
                        const meta = clientMeta.get(guestWs);
                        if (meta) meta.currentSessionId = hostId;

                        guestWs.send(JSON.stringify({
                            type: "JOIN_ACCEPTED",
                            sessionId: hostId,
                            pin: session.pin,
                            hostName: session.hostName,
                            track: session.track
                        }));

                        notifySessionMembersUpdate(hostId);
                    } else {
                        guestWs.send(JSON.stringify({
                            type: "JOIN_DECLINED",
                            message: "Host declined your request"
                        }));
                    }
                }
            }

            // 6. تحديث الأغنية المشغلة من المضيف
            else if (type === "SYNC_TRACK") {
                const sessionId = data.sessionId;
                const session = sessions.get(sessionId);
                if (session && session.hostWs === ws) {
                    session.track = data.track;
                    broadcastToSession(sessionId, {
                        type: "TRACK_CHANGED",
                        track: data.track
                    }, ws);
                }
            }

            // 7. أوامر التشغيل (Play, Pause, Seek, Skip)
            else if (type === "SYNC_ACTION") {
                const sessionId = data.sessionId;
                const session = sessions.get(sessionId);
                if (session && session.hostWs === ws) {
                    broadcastToSession(sessionId, {
                        type: "ACTION_TRIGGERED",
                        action: data.action, // "PAUSE", "RESUME", "SEEK", "STOP"
                        positionMs: data.positionMs,
                        serverEpoch: Date.now()
                    }, ws);
                }
            }

            // 8. طرد لاعب من قبل المضيف
            else if (type === "KICK_USER") {
                const { sessionId, targetId } = data;
                const session = sessions.get(sessionId);
                if (session && session.hostWs === ws) {
                    const listenerIdx = session.listeners.findIndex(l => l.id === targetId);
                    if (listenerIdx !== -1) {
                        const kicked = session.listeners.splice(listenerIdx, 1)[0];
                        if (kicked.ws && kicked.ws.readyState === 1) {
                            kicked.ws.send(JSON.stringify({ type: "KICKED_FROM_SESSION" }));
                        }
                        notifySessionMembersUpdate(sessionId);
                    }
                }
            }

            // 9. الخروج الإرادي من الجلسة
            else if (type === "LEAVE_SESSION") {
                handleUserExit(ws);
            }

        } catch (e) {
            console.error("Packet parse error:", e);
        }
    });

    ws.on('close', () => {
        handleUserExit(ws);
        clientMeta.delete(ws);
    });
});

function handleUserExit(ws) {
    const meta = clientMeta.get(ws);
    if (!meta || !meta.currentSessionId) return;

    const sessionId = meta.currentSessionId;
    const session = sessions.get(sessionId);
    if (!session) return;

    // إذا كان الخارج هو المضيف -> تدمير الجلسة فوراً وإعلام المستمعين
    if (session.hostWs === ws) {
        broadcastToSession(sessionId, { type: "SESSION_DESTROYED", message: "Host left the session" }, ws);
        sessions.delete(sessionId);
    } else {
        // إذا كان الخارج ضيف -> إزالته وإشعار البقية
        session.listeners = session.listeners.filter(l => l.ws !== ws);
        notifySessionMembersUpdate(sessionId);
    }
    meta.currentSessionId = null;
}

// فحص الاتصالات الميتة كل 30 ثانية لتنظيف السيرفر
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (!ws.isAlive) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
    });
}, 30000);
