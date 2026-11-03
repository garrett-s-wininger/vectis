# Documentation site

The production docs server uses `script-src 'self'` and `style-src 'self'`. The classic preset adapter externalizes generated bootstrap scripts and keeps the SVG sprite out of layout using CSS. Code block overrides keep syntax colors in stylesheets. Keep these paths covered by the browser suite: its static server deliberately uses the same CSP as `vectis-docs`.

The `/api` explorer uses pinned, locally bundled Swagger UI assets and `/openapi/v1.json`. It supports operation/schema browsing without live API execution, remote validators, or CDN dependencies. The assets are emitted by the bundler for both production builds and the development server.
