# Baby Name Pairwise

A small web app for ranking baby names with pairwise comparisons, backed by SQLite.

## What It Does

- Stores a shared pool of candidate names.
- Uses URL-based users such as `/troy`, `/grandma`, and `/paula`.
- Tracks separate pairwise-comparison histories and Elo-style rankings for each user.
- Shows each person their own ranking on their personal page.
- Keeps cross-user standings on a separate `/results` page so they do not bias the main comparison flow.

## Tech Stack

- Node.js
- Express
- SQLite via `better-sqlite3`
- Plain HTML, CSS, and JavaScript

## Run Locally

1. Install dependencies:
   `npm install`
2. Start the app:
   `npm start`
3. Open:
   `http://localhost:3000/troy`

## Routes

- `/troy` or `/:userSlug`: comparison page for one user
- `/results`: rankings across all users
- `/api/state/:userSlug`: app state for one user
- `/api/results`: rankings for all users

## Data Storage

The app stores persistent data in a local SQLite database named `baby-names.db`.

Tables:

- `users`: URL-based user slugs
- `names`: shared name pool
- `ratings`: current Elo-style scores per user and name
- `comparisons`: comparison history per user

## Notes

- Unknown user URLs are created the first time you visit or open them.
- The database file is intentionally ignored by git.
- The app can be exposed publicly with a tunnel such as ngrok.
