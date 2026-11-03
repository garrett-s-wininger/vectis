import { useEffect, useRef, useState } from "react";
import Layout from "@theme/Layout";
import Head from "@docusaurus/Head";
import Link from "@docusaurus/Link";
import useBaseUrl from "@docusaurus/useBaseUrl";
import { explorerPlugin } from "../components/APIExplorerPolish";
import { schemaPlugin } from "../components/APISchema";

let bundlePromise;
function loadSwagger(src) {
  if (window.SwaggerUIBundle) return Promise.resolve(window.SwaggerUIBundle);

  bundlePromise ??= new Promise((resolve, reject) => {
    const script = document.createElement("script");
    script.src = src;
    script.onload = () => resolve(window.SwaggerUIBundle);
    script.onerror = () => {
      script.remove();
      bundlePromise = undefined;
      reject(new Error("Unable to load the API explorer."));
    };

    document.head.append(script);
  });

  return bundlePromise;
}

export default function APIExplorer() {
  const container = useRef(null);
  const [error, setError] = useState("");
  const bundleURL = useBaseUrl("/assets/swagger/swagger-ui-bundle.js");
  const stylesheetURL = useBaseUrl("/assets/swagger/swagger-ui.css");
  const specURL = useBaseUrl("/openapi/v1.json");

  useEffect(() => {
    let disposed = false;
    let ui;

    loadSwagger(bundleURL)
      .then((SwaggerUI) => {
        if (disposed) return;
        ui = SwaggerUI({
          domNode: container.current,
          plugins: [schemaPlugin, explorerPlugin],
          url: specURL,
          deepLinking: false,
          filter: true,
          docExpansion: "list",
          displayOperationId: false,
          defaultModelsExpandDepth: 1,
          supportedSubmitMethods: [],
          validatorUrl: null,
          syntaxHighlight: { activated: false }
        });
      })
      .catch((cause) => {
        if (!disposed) setError(cause.message);
      });

    return () => {
      disposed = true;
      ui?.destroy?.();
    };
  }, [bundleURL, specURL]);

  return (
    <Layout title="API explorer" description="Explore Vectis HTTP endpoints, parameters, responses, and schemas.">
      <Head>
        <link rel="stylesheet" href={stylesheetURL} />
      </Head>
      <main className="container margin-vert--lg vectis-api-explorer">
        <h1>API explorer</h1>
        <p>Browse endpoints, expand operations, and inspect request and response schemas.</p>
        <nav className="vectis-api-resources" aria-label="API reference resources">
          <a href="#api-schemas">Schemas</a>
          <Link to="/docs/using/api-reference">API guide</Link>
          <Link to="/docs/using/api-reference#authentication">Authentication</Link>
          <Link to="/docs/using/api-error-code-reference">Error codes</Link>
          <Link to="/docs/using/streaming-reference">Streaming</Link>
          <a href={specURL} download>
            Download OpenAPI JSON
          </a>
        </nav>
        <p className="vectis-api-note">Read-only reference. Requests are not sent to a Vectis installation.</p>
        {error ? (
          <p role="alert">
            {error} <a href={specURL}>Open the schema</a>.
          </p>
        ) : null}
        <div ref={container} />
      </main>
    </Layout>
  );
}
