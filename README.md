# Vavoo proxy test

Private test deployment based on Haehnchen/vavoo-iptv-stream-proxy (MIT).

Endpoints:
- /channels.m3u8 — full catalog
- /channels.m3u8?country=Italy — Italy
- /channels.m3u8?country=United%20Kingdom — UK
- /countries — available countries

Railway start command:
node index.js --http-host 0.0.0.0 --http-port $PORT --vavoo-language en --vavoo-region US --vavoo-url-list both
