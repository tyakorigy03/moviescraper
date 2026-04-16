# moviescraper

## Local setup

- Copy `.env.example` to `.env` and fill in values.
- Install deps: `npm ci`
- Run: `npm run scrape`

## GitHub Actions (public repo)

1. Add repo secrets: `Settings → Secrets and variables → Actions`:
   - `SUPABASE_URL`
   - `SUPABASE_KEY`
   - `TMDB_API_KEY`
2. Allow the workflow to push commits:
   - `Settings → Actions → General → Workflow permissions → Read and write permissions`
3. Workflows:
   - `.github/workflows/scraper.yml` (scheduled every 30 minutes + manual)
   - `.github/workflows/scraper1.yml` (manual `scrape2`)

The workflows set `SUPABASE_INSERT_ONLY=true` so scheduled runs don’t keep updating existing rows.
