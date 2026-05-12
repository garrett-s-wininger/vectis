import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';

function Hero() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header
      style={{
        padding: '4rem 2rem',
        textAlign: 'center',
        background: 'linear-gradient(135deg, #2e8555 0%, #1a4d33 100%)',
        color: '#fff',
      }}>
      <h1 style={{fontSize: '3rem', margin: 0}}>{siteConfig.title}</h1>
      <p style={{fontSize: '1.25rem', margin: '1rem 0 2rem', opacity: 0.9}}>
        {siteConfig.tagline}
      </p>
      <div>
        <Link
          to="/docs/user/api-reference"
          style={{
            display: 'inline-block',
            padding: '0.75rem 1.5rem',
            margin: '0.5rem',
            background: 'rgba(255,255,255,0.15)',
            color: '#fff',
            borderRadius: '4px',
            fontWeight: 600,
            textDecoration: 'none',
          }}>
          API Reference
        </Link>
        <Link
          to="/docs/operator/configuration"
          style={{
            display: 'inline-block',
            padding: '0.75rem 1.5rem',
            margin: '0.5rem',
            background: 'rgba(255,255,255,0.15)',
            color: '#fff',
            borderRadius: '4px',
            fontWeight: 600,
            textDecoration: 'none',
          }}>
          Configuration
        </Link>
        <Link
          to="/docs/developer/architecture"
          style={{
            display: 'inline-block',
            padding: '0.75rem 1.5rem',
            margin: '0.5rem',
            background: 'rgba(255,255,255,0.15)',
            color: '#fff',
            borderRadius: '4px',
            fontWeight: 600,
            textDecoration: 'none',
          }}>
          Architecture
        </Link>
      </div>
    </header>
  );
}

function Section({title, description, link, linkText}) {
  return (
    <div
      style={{
        flex: '1 1 300px',
        padding: '1.5rem',
        margin: '0.5rem',
        border: '1px solid #e0e0e0',
        borderRadius: '8px',
      }}>
      <h2>{title}</h2>
      <p style={{color: '#666'}}>{description}</p>
      <Link to={link} style={{fontWeight: 600}}>
        {linkText || `Browse ${title}`} &rarr;
      </Link>
    </div>
  );
}

export default function Home() {
  return (
    <Layout description="Vectis documentation — self-hosted build/CI system">
      <Hero />
      <main
        style={{
          maxWidth: 1100,
          margin: '0 auto',
          padding: '2rem',
          display: 'flex',
          flexWrap: 'wrap',
          justifyContent: 'center',
        }}>
        <Section
          title="User Guide"
          description="API reference, job validation, idempotency, and log streaming for integrators consuming the Vectis REST surface."
          link="/docs/user/api-reference"
        />
        <Section
          title="Operator Guide"
          description="Configuration, deployment, runbooks, repair procedures, scaling, backup/restore, and capacity planning."
          link="/docs/operator/configuration"
        />
        <Section
          title="Developer Guide"
          description="Architecture, planning, migration rules, ADRs, and design decisions for contributors."
          link="/docs/developer/architecture"
        />
      </main>
    </Layout>
  );
}
