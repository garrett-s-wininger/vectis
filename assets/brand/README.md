# Brand Assets

This directory is the shared brand asset source for Vectis sites and UI builds.

Files in `public/` are web-ready assets served through `/img/...` by the docs
site and the console UI. Files in `source/` are editable working assets and should
not be referenced directly by application code or exposed through site static
roots.

Current asset roles:

- `public/favicon.svg`: optimized browser favicon.
- `public/vectis.png`: light-mode wordmark used by the docs site and console UI.
- `public/vectis-dark.png`: dark-mode wordmark used by the docs site.
- `source/vectis.svg`: editable source for the light-mode wordmark.
- `source/vectis-dark.svg`: editable source for the dark-mode wordmark.
