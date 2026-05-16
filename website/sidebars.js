const sidebars = {
  docs: [
    'getting-started',
    {
      type: 'category',
      label: 'Concepts',
      items: [
        'concepts/architecture',
        'concepts/glossary',
        'concepts/security',
        'concepts/internal-service-trust',
        'concepts/failure-domains',
        'concepts/compatibility',
      ],
    },
    {
      type: 'category',
      label: 'Using Vectis',
      items: [
        'using/your-first-job',
        'using/cli-guide',
        'using/api-reference',
        'using/job-validation',
        'using/log-streaming',
        'using/idempotency-and-retries',
      ],
    },
    {
      type: 'category',
      label: 'Operating Vectis',
      items: [
        'operating/configuration',
        {
          type: 'category',
          label: 'Deployment',
          items: [
            'operating/deployment/reference-deployment-posture',
            'operating/deployment/trusted-proxy-client-ip',
            'operating/deployment/secrets-and-redaction',
            'operating/deployment/scaling-and-restarts',
          ],
        },
        {
          type: 'category',
          label: 'Reliability',
          items: [
            'operating/reliability/runbooks',
            'operating/reliability/repair-runbooks',
            'operating/reliability/dispatch-visibility',
            'operating/reliability/backup-restore',
            'operating/reliability/retention',
          ],
        },
        {
          type: 'category',
          label: 'Capacity',
          items: [
            'operating/capacity/capacity-load-envelope',
            'operating/capacity/capacity-drills',
          ],
        },
        {
          type: 'category',
          label: 'Reference',
          items: [
            'operating/reference/cli-operational-coverage',
            'operating/reference/doctor-check-catalog',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Developing Vectis',
      items: [
        'developing/contexts',
        'developing/actions',
        'developing/migrations',
        'developing/retry-policy',
        'developing/releases',
        {
          type: 'category',
          label: 'Roadmap',
          items: [
            'developing/roadmap/foundation-roadmap',
            'developing/roadmap/planning',
            'developing/roadmap/federation',
          ],
        },
        {
          type: 'category',
          label: 'Architecture Decisions',
          items: [
            'developing/architecture-decisions/index',
            'developing/architecture-decisions/async-enqueue-after-http-202',
            'developing/architecture-decisions/standalone-reconciler-process',
            'developing/architecture-decisions/database-claims-and-queue-deliveries',
            'developing/architecture-decisions/migration-compatibility-and-rollback',
            'developing/architecture-decisions/gossip-based-ha-registry',
          ],
        },
      ],
    },
  ],
};

module.exports = sidebars;
