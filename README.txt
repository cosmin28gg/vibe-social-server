WEBSOCKET SERVER FOR VIBE SOCIAL
================================

QUICK START:
1. npm install
2. Copy .env.example to .env and add your Supabase credentials
3. npm start

DEPLOY TO RAILWAY (EASIEST):
1. Push this folder to GitHub
2. Go to railway.app
3. Click "New Project" -> "Deploy from GitHub"
4. Select your repo
5. Add environment variables (SUPABASE_URL, SUPABASE_SERVICE_KEY, PORT)
6. Deploy
7. Copy the public URL (e.g., https://your-app.railway.app)
8. Update Flutter app: websocket_service.dart line 48 with your URL

DEPLOY TO RENDER:
1. Push to GitHub
2. Go to render.com
3. New Web Service -> Connect GitHub
4. Add env vars
5. Deploy

DEPLOY TO DIGITALOCEAN:
1. Create droplet ($5/month)
2. SSH in
3. Install Node.js: curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash - && sudo apt-get install -y nodejs
4. Clone repo
5. npm install
6. Install PM2: npm install -g pm2
7. pm2 start server.js --name vibe-chat
8. pm2 startup && pm2 save

FLUTTER SETUP:
Update lib/chat/services/websocket_service.dart line 48:
const wsUrl = 'wss://your-server-url.com'; // Use wss:// for production (secure)

Then in main.dart:
await WebSocketService.instance.connect();
