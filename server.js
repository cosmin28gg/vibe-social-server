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

        // Send push notifications for offline users
        if (offlineTo.length > 0) {
          console.log(`  🔔 Triggering FCM for ${offlineTo.length} offline recipients`);
          
          // Fetch sender info and send notifications (async, non-blocking)
          (async () => {
            let senderUsername = 'Someone';
            let senderAvatarUrl = null;
            
            try {
              const { data: senderData } = await supabase
                .from('users')
                .select('username, avatar_url')
                .eq('id', userId)
                .single();
              
              if (senderData) {
                senderUsername = senderData.username || 'Someone';
                senderAvatarUrl = senderData.avatar_url;
              }
            } catch (err) {
              console.error(`  ⚠️ Failed to fetch sender info: ${err.message}`);
            }
            
            const msgType = completeMessage.type || 'text';
            const msgText = message.message_data?._preview || message.message_data?._plaintext || 'New message';
            
            // Check if group chat
            const isGroupChat = convo_id.startsWith('chatgrp_');
            let groupId = null;
            let groupName = null;
            
            if (isGroupChat) {
              groupId = convo_id.replace('chatgrp_', '');
              // Fetch group name
              try {
                const { data } = await supabase
                  .from('conversations')
                  .select('name')
                  .eq('id', convo_id)
                  .single();
                groupName = data?.name;
              } catch (err) {
                console.error(`  ⚠️ Failed to fetch group name: ${err.message}`);
              }
            }
            
            // Send FCM to each offline recipient
            for (const recipientId of offlineTo) {
              supabase.functions
                .invoke('push', {
                  body: {
                    recipient_id: recipientId,
                    type: 'message',
                    sender_id: userId,
                    sender_username: senderUsername,
                    sender_avatar_url: senderAvatarUrl,
                    msg_type: msgType,
                    msg_text: msgText,
                    is_group_chat: isGroupChat,
                    group_id: groupId,
                    group_name: groupName,
                  },
                })
                .then(({ data, error }) => {
                  if (error) {
                    console.error(`  ❌ FCM failed for ${recipientId}:`, error);
                  } else {
                    const sent = data?.sent || 0;
                    console.log(`  ✅ FCM sent to ${recipientId} (${sent} devices)`);
                  }
                })
                .catch(err => console.error(`  ❌ FCM error for ${recipientId}:`, err.message));
            }
          })();
        }
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
            const seenAt = new Date().toISOString();
            
            // Notify sender
            const senderWs = connections.get(sender_id);
            if (senderWs && senderWs.readyState === WebSocket.OPEN) {
              senderWs.send(JSON.stringify({
                type: 'message_seen',
                message_id,
                convo_id,
                seen_by: userId,
                seen_at: seenAt,
              }));
            }
            
            // Also notify the person who marked it as seen (for chat list updates)
            const readerWs = connections.get(userId);
            if (readerWs && readerWs.readyState === WebSocket.OPEN) {
              readerWs.send(JSON.stringify({
                type: 'message_seen',
                message_id,
                convo_id,
                seen_by: userId,
                seen_at: seenAt,
              }));
            }
          })
          .catch(err => console.error(`Mark seen failed: ${err.message}`));
      }

      // Handle message deletion
      if (message.type === 'message_deleted') {
        const { message_id, convo_id, recipient_ids } = message;
        
        console.log(`🗑️ Message deleted: ${message_id} in convo ${convo_id}`);
        
        // Forward deletion to all recipients
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'message_deleted',
              message_id,
              convo_id,
            }));
            console.log(`  ✅ Notified ${recipientId} of deletion`);
          } else {
            console.log(`  📥 Recipient ${recipientId} offline - deletion will sync on reconnect`);
          }
        }
      }

      // Handle sticker placement
      if (message.type === 'place_sticker') {
        const { convo_id, recipient_ids, sticker_data } = message;
        
        console.log(`🎨 Sticker placed in ${convo_id}`);
        
        // Broadcast to all recipients
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'sticker_placed',
              convo_id,
              sticker_data,
              placed_by: userId,
            }));
          }
        }
        
        // Save to DB (background)
        supabase
          .from('placed_stickers')
          .insert({
            convo_id,
            user_id: userId,
            ...sticker_data,
          })
          .catch(err => console.error(`Sticker save failed: ${err.message}`));
      }

      // Handle sticker deletion
      if (message.type === 'delete_sticker') {
        const { convo_id, recipient_ids, sticker_id } = message;
        
        console.log(`🗑️ Sticker deleted from ${convo_id}`);
        
        // Broadcast to all recipients
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'sticker_deleted',
              convo_id,
              sticker_id,
              deleted_by: userId,
            }));
          }
        }
        
        // Delete from DB (background)
        supabase
          .from('placed_stickers')
          .delete()
          .eq('id', sticker_id)
          .catch(err => console.error(`Sticker delete failed: ${err.message}`));
      }

      // Handle reactions
      if (message.type === 'add_reaction') {
        const { message_id, convo_id, recipient_ids, emoji, sound_url } = message;
        
        console.log(`😊 Reaction ${emoji} added to message ${message_id}`);
        
        // Broadcast to all recipients
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'reaction_added',
              message_id,
              convo_id,
              emoji,
              reactor_id: userId,
              sound_url,
            }));
          }
        }
        
        // Save to DB (background)
        supabase
          .from('message_reactions')
          .insert({
            message_id,
            user_id: userId,
            emoji,
            sound_url,
          })
          .catch(err => console.error(`Reaction save failed: ${err.message}`));
      }

      // Handle reaction removal
      if (message.type === 'remove_reaction') {
        const { message_id, convo_id, recipient_ids, emoji } = message;
        
        console.log(`🗑️ Reaction ${emoji} removed from message ${message_id}`);
        
        // Broadcast to all recipients
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'reaction_removed',
              message_id,
              convo_id,
              emoji,
              reactor_id: userId,
            }));
          }
        }
        
        // Delete from DB (background)
        supabase
          .from('message_reactions')
          .delete()
          .eq('message_id', message_id)
          .eq('user_id', userId)
          .eq('emoji', emoji)
          .catch(err => console.error(`Reaction delete failed: ${err.message}`));
      }

      // Handle GIF/sticker/audio sticker messages (same as text messages)
      if (message.type === 'send_media_message') {
        const {
          convo_id,
          recipient_ids,
          message_data,
        } = message;

        const messageId = message_data.id || uuidv4();
        const timestamp = new Date().toISOString();

        const completeMessage = {
          id: messageId,
          convo_id,
          sender_id: userId,
          created_at: timestamp,
          seen_at: null,
          ...message_data,
        };

        console.log(`📸 Media message (${message_data.type}) from ${userId} to ${convo_id}`);

        // Send to all online recipients INSTANTLY
        const deliveredTo = [];
        const offlineTo = [];

        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'new_message',
              message: completeMessage,
            }));
            deliveredTo.push(recipientId);
            console.log(`  ✅ Delivered to ${recipientId}`);
          } else {
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

        // Send delivery confirmation
        ws.send(JSON.stringify({
          type: 'message_sent',
          message_id: messageId,
          delivered_to: deliveredTo,
          queued_for: offlineTo,
        }));

        // Save to DB (background)
        supabase
          .from('messages')
          .insert(completeMessage)
          .then(() => console.log(`  💾 Saved to DB: ${messageId}`))
          .catch(err => console.error(`  ❌ DB save failed: ${err.message}`));
      }

      // Handle drawing stroke
      if (message.type === 'draw_stroke') {
        const { convo_id, recipient_ids, stroke, page } = message;
        
        console.log(`🎨 Drawing stroke on page ${page} in ${convo_id}`);
        
        // Broadcast to all recipients
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'draw_stroke',
              convo_id,
              stroke,
              page,
              sender_id: userId,
            }));
          }
        }
        
        // Save to DB (background)
        supabase
          .from('drawing_sessions')
          .select('pages_json')
          .eq('convo_id', convo_id)
          .maybeSingle()
          .then(res => {
            let pagesData = [];
            if (res && res.pages_json) {
              pagesData = typeof res.pages_json === 'string' 
                ? JSON.parse(res.pages_json) 
                : res.pages_json;
            }
            
            // Ensure page exists
            while (pagesData.length <= page) {
              pagesData.push({ strokes: [] });
            }
            
            // Add stroke
            if (!pagesData[page].strokes) {
              pagesData[page].strokes = [];
            }
            pagesData[page].strokes.push(stroke);
            
            // Save back
            return supabase
              .from('drawing_sessions')
              .upsert({
                convo_id,
                pages_json: pagesData,
                updated_at: new Date().toISOString(),
              });
          })
          .catch(err => console.error(`Drawing save failed: ${err.message}`));
      }

      // Handle drawing clear
      if (message.type === 'draw_clear') {
        const { convo_id, recipient_ids, page } = message;
        
        console.log(`🗑️ Drawing cleared on page ${page} in ${convo_id}`);
        
        // Broadcast to all recipients
        for (const recipientId of recipient_ids) {
          if (recipientId === userId) continue;
          
          const recipientWs = connections.get(recipientId);
          if (recipientWs && recipientWs.readyState === WebSocket.OPEN) {
            recipientWs.send(JSON.stringify({
              type: 'draw_clear',
              convo_id,
              page,
              sender_id: userId,
            }));
          }
        }
        
        // Clear in DB (background)
        supabase
          .from('drawing_sessions')
          .select('pages_json')
          .eq('convo_id', convo_id)
          .maybeSingle()
          .then(res => {
            let pagesData = [];
            if (res && res.pages_json) {
              pagesData = typeof res.pages_json === 'string' 
                ? JSON.parse(res.pages_json) 
                : res.pages_json;
            }
            
            // Clear page
            if (pagesData[page]) {
              pagesData[page].strokes = [];
            }
            
            // Save back
            return supabase
              .from('drawing_sessions')
              .upsert({
                convo_id,
                pages_json: pagesData,
                updated_at: new Date().toISOString(),
              });
          })
          .catch(err => console.error(`Drawing clear failed: ${err.message}`));
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
