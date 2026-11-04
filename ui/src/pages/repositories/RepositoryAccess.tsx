import { useEffect, useRef, useState } from "react";
import { Button, DataTable, ErrorAlert, SelectField } from "../../components";
import type { Namespace } from "../../domain/console";
import type { RepositorySource, RepositoryGrant } from "../../data/repositories";
import styles from "../RepositoriesPage.module.css";

export function RepositoryAccess({ id, source }: { id: string; source: RepositorySource }) {
  const [namespaces, setNamespaces] = useState<Namespace[]>([]);
  const [grants, setGrants] = useState<RepositoryGrant[] | null>(null);
  const [selected, setSelected] = useState("");
  const [pending, setPending] = useState<string | null>(null);
  const [confirmation, setConfirmation] = useState<number | null>(null);
  const [error, setError] = useState("");
  const lock = useRef(false);

  useEffect(() => {
    let ignore = false;
    Promise.all([source.namespaces(), source.access(id)])
      .then(([rows, bindings]) => {
        if (!ignore) {
          setNamespaces(rows);
          setGrants(bindings);
        }
      })
      .catch((cause) => {
        if (!ignore) setError(cause instanceof Error ? cause.message : "Unable to load namespace access.");
      });

    return () => {
      ignore = true;
    };
  }, [id, source]);

  const choices = namespaces.filter((row) => !grants?.some((grant) => grant.namespace_id === row.id));
  const selectedID = choices.some((row) => String(row.id) === selected) ? selected : String(choices[0]?.id ?? "");

  async function change(namespaceID: number, grant: boolean) {
    if (lock.current) return;

    lock.current = true;
    setPending(grant ? "grant" : String(namespaceID));
    setError("");

    try {
      setGrants(await (grant ? source.grant(id, namespaceID) : source.revoke(id, namespaceID)));
      setSelected("");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Unable to update namespace access.");
    } finally {
      lock.current = false;
      setPending(null);
      setConfirmation(null);
    }
  }

  return (
    <section
      className={`${styles.panel} polished-panel polished-panel--accent-top`}
      aria-labelledby="repository-access-title"
    >
      <div className={styles.sectionCopy}>
        <h2 id="repository-access-title">Namespace access</h2>
        <p>
          Grant namespaces permission to use this connection. Their roles govern who can read or run jobs; connection
          administration stays with instance administrators.
        </p>
      </div>
      <ErrorAlert title="Unable to Update Namespace Access" message={error} />
      {grants === null && !error ? (
        <p role="status">Loading namespace access…</p>
      ) : grants !== null ? (
        <>
          {choices.length ? (
            <form
              className={styles.accessForm}
              onSubmit={(event) => {
                event.preventDefault();
                if (selectedID) void change(Number(selectedID), true);
              }}
            >
              <SelectField
                label="Grant access to namespace"
                name="repositoryNamespace"
                disabled={pending !== null}
                value={selectedID}
                onChange={(event) => setSelected(event.target.value)}
                options={choices.map((row) => ({ value: String(row.id), label: row.path === "/" ? "Root" : row.path }))}
              />
              <Button
                type="submit"
                disabled={!selectedID || pending !== null}
                pending={pending === "grant"}
                pendingLabel="Granting…"
              >
                Grant access
              </Button>
            </form>
          ) : (
            <p>All namespaces already have access.</p>
          )}
          <DataTable
            variant="inset"
            rows={grants}
            getRowKey={(row) => String(row.namespace_id)}
            emptyMessage="No namespaces can use this connection."
            columns={[
              { header: "Namespace", cell: (row) => (row.namespace === "/" ? "Root" : row.namespace) },
              {
                header: "Actions",
                width: "180px",
                align: "end",
                cell: (row) => (
                  <Button
                    variant="quiet"
                    disabled={pending !== null}
                    pending={pending === String(row.namespace_id)}
                    pendingLabel="Revoking…"
                    onBlur={() => {
                      if (!lock.current) setConfirmation(null);
                    }}
                    onKeyDown={(event) => {
                      if (event.key === "Escape") setConfirmation(null);
                    }}
                    onClick={() => {
                      if (confirmation === row.namespace_id) void change(row.namespace_id, false);
                      else setConfirmation(row.namespace_id);
                    }}
                  >
                    {confirmation === row.namespace_id ? "Confirm revoke" : "Revoke access"}
                  </Button>
                )
              }
            ]}
          />
        </>
      ) : null}
    </section>
  );
}
