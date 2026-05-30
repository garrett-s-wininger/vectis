import type { Meta, StoryObj } from "@storybook/react-vite";
import type { ReactNode } from "react";
import styles from "./TokenInventory.module.css";

type Token = {
  name: string;
  value: string;
};

const colors: Token[] = [
  { name: "surface-page", value: "var(--surface-page)" },
  { name: "surface-panel", value: "var(--surface-panel)" },
  { name: "surface-subtle", value: "var(--surface-subtle)" },
  { name: "surface-selected", value: "var(--surface-selected)" },
  { name: "text-primary", value: "var(--text-primary)" },
  { name: "text-secondary", value: "var(--text-secondary)" },
  { name: "text-muted", value: "var(--text-muted)" },
  { name: "action-primary-bg", value: "var(--action-primary-bg)" },
  { name: "status-success-bg", value: "var(--status-success-bg)" },
  { name: "status-warning-bg", value: "var(--status-warning-bg)" },
  { name: "status-danger-bg", value: "var(--status-danger-bg)" },
  { name: "status-neutral-bg", value: "var(--status-neutral-bg)" }
];

const typeScale: Token[] = [
  { name: "font-size-xs", value: "var(--font-size-xs)" },
  { name: "font-size-sm", value: "var(--font-size-sm)" },
  { name: "font-size-md", value: "var(--font-size-md)" },
  { name: "font-size-lg", value: "var(--font-size-lg)" },
  { name: "font-size-xl", value: "var(--font-size-xl)" },
  { name: "font-size-2xl", value: "var(--font-size-2xl)" },
  { name: "font-size-3xl", value: "var(--font-size-3xl)" },
  { name: "font-size-4xl", value: "var(--font-size-4xl)" }
];

const spacing: Token[] = [
  { name: "space-3xs", value: "var(--space-3xs)" },
  { name: "space-2xs", value: "var(--space-2xs)" },
  { name: "space-xs", value: "var(--space-xs)" },
  { name: "space-sm", value: "var(--space-sm)" },
  { name: "space-md", value: "var(--space-md)" },
  { name: "space-lg", value: "var(--space-lg)" },
  { name: "space-xl", value: "var(--space-xl)" },
  { name: "space-2xl", value: "var(--space-2xl)" },
  { name: "space-3xl", value: "var(--space-3xl)" },
  { name: "space-5xl", value: "var(--space-5xl)" }
];

const radii: Token[] = [
  { name: "radius-control", value: "var(--radius-control)" },
  { name: "radius-surface", value: "var(--radius-surface)" },
  { name: "radius-pill", value: "var(--radius-pill)" }
];

function Section({
  children,
  description,
  title
}: {
  children: ReactNode;
  description: string;
  title: string;
}) {
  return (
    <section className={styles.section}>
      <div className={styles.sectionHeader}>
        <h2>{title}</h2>
        <p>{description}</p>
      </div>
      {children}
    </section>
  );
}

function TokenInventory() {
  return (
    <main className={styles.root}>
      <Section description="Semantic color roles currently available to components." title="Colors">
        <div className={styles.grid}>
          {colors.map((token) => (
            <div className={styles.swatch} key={token.name}>
              <div className={styles.chip} style={{ background: token.value }} />
              <span className={styles.name}>--{token.name}</span>
              <span className={styles.value}>{token.value}</span>
            </div>
          ))}
        </div>
      </Section>

      <Section description="The type scale before any visual tightening." title="Type Scale">
        <div className={styles.grid}>
          {typeScale.map((token) => (
            <div className={styles.sample} key={token.name}>
              <p className={styles.typeSample} style={{ fontSize: token.value }}>
                Vectis dashboard
              </p>
              <span className={styles.name}>--{token.name}</span>
              <span className={styles.value}>{token.value}</span>
            </div>
          ))}
        </div>
      </Section>

      <Section description="Spacing values currently used by layout and component modules." title="Spacing">
        <div className={styles.section}>
          {spacing.map((token) => (
            <div className={styles.spacingRow} key={token.name}>
              <span className={styles.name}>--{token.name}</span>
              <div className={styles.spacingBar} style={{ width: token.value }} />
              <span className={styles.value}>{token.value}</span>
            </div>
          ))}
        </div>
      </Section>

      <Section description="Radius and shadow primitives used for controls and surfaces." title="Shape">
        <div className={styles.grid}>
          {radii.map((token) => (
            <div className={styles.sample} key={token.name}>
              <div className={styles.radiusBox} style={{ borderRadius: token.value }} />
              <span className={styles.name}>--{token.name}</span>
              <span className={styles.value}>{token.value}</span>
            </div>
          ))}
          <div className={styles.sample}>
            <div className={styles.shadowBox} />
            <span className={styles.name}>--panel-shadow-raised</span>
            <span className={styles.value}>var(--panel-shadow-raised)</span>
          </div>
        </div>
      </Section>
    </main>
  );
}

const meta = {
  title: "Design/Tokens",
  render: () => <TokenInventory />
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

export const Inventory: Story = {};
