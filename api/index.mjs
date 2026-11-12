// Vercel serverless entry. All /api/* requests are rewritten here (see vercel.json); the app strips the /api prefix.
import { createApp } from '../server/server.mjs';
const app = createApp(process.env.DATABASE_URL);
export default (req, res) => app.handler(req, res);
