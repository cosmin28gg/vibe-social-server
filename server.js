const WebSocket = require('ws');
const { createClient } = require('@supabase/supabase-js');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

const PORT = process.env.PORT || 8080;
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_KEY
);

// Store active connections: userId -> WebSocket
const connections = new Map();

// Store offline message queues: userId -> [messages]
const offlineQueues = new Map();

const wss = new WebSocket.Server({ port: PORT });

console.log(`🚀 WebSocket server running on port ${PORT}`);

wss.on('connection', (ws) => {
  let userId = null;
  let isAuthenticated = false;

  console.log('📱 New connection attempt');

  ws.on('message', async (data) => {
    try {
      const message = JSON.parse(data.toString());
      
      // Handle authentication
      if (message.type === 'auth') {
        const { token, user_id } = message;
        
        // Verify token with Supabase
        const { data: { user }, error } = await supabase.auth.getUser(token);
        
        if (error || !user || user.id !== user_id) {
          ws.send(JSON.stringify({ type: 'auth_error', error: 'Invalid token' }));
          ws.close();
          return;
        }
        
        userId = user_id;
        isAuthenticated = true;
        connections.set(userId, ws);
        
        console.log(`✅ User ${userId} authenticated`);
        ws.send(JSON.stringify({ type: 'auth_success', user_id: userId }));
        
        // Send any queued offline messages
        const queue = offlineQueues.get(userId) || [];
        if (queue.length > 0) {
          console.log(`📬 Sending ${queue.length} queued messages to ${userId}`);
          queue.forEach(msg => ws.send(JSON.stringify(msg)));
          offlineQueues.delete(userId);
        }
        
        return;
      }

      if (!isAuthenticated) {
        ws.send(JSON.stringify({ type: 'error', error: 'Not authenticated' }));
        return;
      }

      // Handle sending a message
      if (message.type === 'send_message') {
        const {
          convo_id,
          recipient_ids, // Array of recipient user IDs
          message_data,
        } = message;

        // Generate message ID
        const messageId = message_data.id || uuidv4();
        const timestamp = new Date().toISOString();

        // Create complete message object
        const completeMessage = {
          id: messageId,
          convo_id,
          sender_id: userId,
          created_at: timestamp,
          seen_at: null,
          ...message_data,
        };

        console.log(`📨 Message from ${userId} to convo ${convo_id}`);

        // Send to all online recipients INSTANTLY
        const deliveredTo = [];
        const offlineTo = [];

        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue; // Don't send to self
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'new_message',
              message: completeMessage,
            }));
            deliveredTo.push(recipientId);
            console.log(`  ✅ Delivered to ${recipientId}`);
          } else {
            // Queue for offline user
            if (!offlineQueues.has(recipientId)) {
              offlineQueues.set(recipientId, []);
            }
            offlineQueues.get(recipientId).push({
              type: 'new_message',
              message: completeMessage,
            });
            offlineTo.push(recipientId);
            console.log(`  📥 Queued for offline user ${recipientId}`);
          }
        }

        // Send delivery confirmation to sender
        ws.send(JSON.stringify({
          type: 'message_sent',
          message_id: messageId,
          delivered_to: deliveredTo,
          queued_for: offlineTo,
        }));

        // Background: Save to Supabase (don't wait for this)
        supabase
          .from('messages')
          .insert(completeMessage)
          .then(() => console.log(`  💾 Saved to DB: ${messageId}`))
          .catch(err => console.error(`  ❌ DB save failed: ${err.message}`));

        // Optional: Send push notifications for offline users
        // You can implement this by fetching FCM tokens from Supabase
        // and sending via Firebase Admin SDK
        // For now, let the client handle push notifications
      }

      // Handle typing indicators
      if (message.type === 'typing') {
        const { convo_id, recipient_ids, is_typing } = message;
        
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'typing',
              convo_id,
              user_id: userId,
              is_typing,
            }));
          }
        }
      }

      // Handle message seen
      if (message.type === 'mark_seen') {
        const { message_id, convo_id, sender_id } = message;
        
        // Update in DB (background)
        supabase
          .from('messages')
          .update({ seen_at: new Date().toISOString() })
          .eq('id', message_id)
          .then(() => {
            // Notify sender
            const senderWs = connections.get(sender_id);
            if (senderWs && senderWs.readyState === WebSocket.OPEN) {
              senderWs.send(JSON.stringify({
                type: 'message_seen',
                message_id,
                seen_by: userId,
                seen_at: new Date().toISOString(),
              }));
            }
          })
          .catch(err => console.error(`Mark seen failed: ${err.message}`));
      }

    } catch (error) {
      console.error('❌ Message handling error:', error);
      ws.send(JSON.stringify({ type: 'error', error: error.message }));
    }
  });

  ws.on('close', () => {
    if (userId) {
      connections.delete(userId);
      console.log(`👋 User ${userId} disconnected`);
    }
  });

  ws.on('error', (error) => {
    console.error('WebSocket error:', error);
  });
});

// Heartbeat to detect dead connections
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.isAlive === false) {
      return ws.terminate();
    }
    ws.isAlive = false;
    ws.ping();
  });
}, 30000);

wss.on('connection', (ws) => {
  ws.isAlive = true;
  ws.on('pong', () => {
    ws.isAlive = true;
  });
});
