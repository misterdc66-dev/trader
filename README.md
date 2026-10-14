# muse-fake-price-test

Connection test only. This service sends **fake, clearly labeled prices** once per
second so we can verify whether a Muse dashboard client can hold a secure live
connection to a Render Web Service and render updates at a 1–2 second cadence.

- No real market data. No Tradier or any other vendor credentials.
- Endpoints: `/` self-test page, `/health`, `/stream` (Server-Sent Events), `/ws` (WebSocket).
- Every payload carries `fake: true` and the label "FAKE TEST DATA - NOT REAL MARKET DATA",
  plus a sequence number and the server's send timestamp so the client can measure delay.
