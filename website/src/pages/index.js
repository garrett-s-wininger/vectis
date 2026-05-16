import Link from '@docusaurus/Link';
import Layout from '@theme/Layout';

function Hero() {
  return (
    <header className="vectis-hero">
      <div className="vectis-hero__inner">
        <h1 className="vectis-hero__title">Modern Self-Hosted Build Infrastructure</h1>
        <p className="vectis-hero__lead">
          Run workflows on infrastructure you control. Vectis gives you local hosting,
          replaceable components, and an API-first design you can adapt to your environment.
        </p>
        <div className="vectis-hero__actions">
          <Link className="vectis-hero__cta vectis-hero__cta--primary" to="/docs/operating/configuration">
            Deploy
          </Link>
        </div>
      </div>
    </header>
  );
}

function Section({title, description, link, linkText}) {
  return (
    <article className="vectis-home-section">
      <h2 className="vectis-home-section__title">{title}</h2>
      <p className="vectis-home-section__body">{description}</p>
      <Link to={link} className="vectis-home-section__link">
        {linkText || `Browse ${title}`} &rarr;
      </Link>
    </article>
  );
}

export default function Home() {
  return (
    <Layout description="Vectis documentation — self-hosted build/CI system">
      <Hero />
      <div className="vectis-home-band">
        <main className="vectis-home-main">
          <Section
            title="Use the API"
            description="Manage jobs, runs, and logs with Vectis' built-in API."
            link="/docs/using/api-reference"
            linkText="Open API guide"
          />
          <Section
            title="Maintain Vectis"
            description="Learn how to ensure the health, performance, and security of your Vectis installation."
            link="/docs/operating/configuration"
            linkText="Open operator guide"
          />
          <Section
            title="Extend the System"
            description="Examine the architecture, understand its codebase, and extend it to fit your needs."
            link="/docs/concepts/architecture"
            linkText="Open developer guide"
          />
        </main>
      </div>
    </Layout>
  );
}
