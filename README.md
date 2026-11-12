# Investa

Investa is a graduation-project simulation of an investing platform. Founders list a startup and say what percent of the company they offer. Investors send interest requests and offers. A share is recorded only when both sides agree on identical terms.

**No real money moves, and recorded agreements are not legal equity.** Everything here is for demonstration.

**Live demo:** https://investa-live.vercel.app/app/ (Arabic and English, right-to-left layout, light and dark mode)

Graduation team project. I built the React front end and led a team of three developers.

## How the demo works

1. Create an account and choose a role, **Investor** or **Founder**.
2. A founder creates a startup page, sets the percent of the company on offer and adds products.
3. An investor opens a startup and taps **Express interest**.
4. The founder accepts or declines. Accepting opens a chat between the two.
5. The investor sends an offer: an amount and a percent of the company. Only one offer can be open at a time, and an open offer expires after 7 days.
6. The founder accepts, declines or counters. A counteroffer needs the investor's confirmation, and the proposer can withdraw an open offer.
7. A share is recorded only when the other side accepts the exact terms on the table. If the terms changed in the meantime, the accept is rejected.
8. The total of recorded agreements can never go above the percent the founder offered. The API and a database rule both enforce this.

## What is in the app

- **Public pages:** landing page, investor FAQ, team page and a "Raise money" page.
- **Accounts:** register, sign in, change password, forgot and reset password screens.
- **Founder view:** startup page, products (add, edit, delete) and a cap table, visible only to the founder, showing the share offered, agreed and remaining.
- **Investor view:** discover startups, interest requests, offers, and a portfolio of recorded agreements.
- **Messages:** chat that opens after an interest request is accepted.
- **Dashboard, reports, invoices, finances and transactions,** with a **Recent Activities** log of requests, offers and agreements.
- **Notifications, profile and settings.**
- **Assistant:** a scripted demo that answers a fixed set of questions about the offer journey. It is not a language model.

## Limits

- It is a simulation. Payments, valuations and investment returns are not calculated or tracked.
- Some figures on the home page and in charts are labelled sample data from the original design.
- Store badges, social links and company statistics from the original design are left out.
- Password-reset email is switched off on the live demo (no mail provider is configured), so reset links are not sent. The code supports sending them through Brevo once `BREVO_API_KEY` and `MAIL_FROM` are set.
- Use a test email and a throwaway password. Registration needs a valid-looking email and a password of at least 8 characters. Passwords are stored hashed (scrypt), and the API limits requests per IP.

## Tech stack

React, TypeScript and Vite on the front end. A dependency-light Node API (plain `node:http`, `pg` for Postgres) that also runs on SQLite locally. Postgres on Neon, hosted on Vercel.

## Credits

Investa started as a team graduation project. This version rebuilds the front end and adds a working backend so the full journey runs end to end.
