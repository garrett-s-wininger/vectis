import type { ReactNode } from "react";
import { BreadcrumbTrail, Button, PageHeader } from "../../components";
import type { User, UserStatus } from "../../domain/console";
import { ResourceStatus } from "../shared";
import styles from "../UsersPage.module.css";
import { roleTone } from "./UserPresentation";

type UserDetailPageProps = {
  children?: ReactNode;
  onBack: () => void;
  onRemoveUser: (user: User) => void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => Promise<void> | void;
  user: User;
};

export function UserDetailPage({ children, onBack, onRemoveUser, onUpdateUserStatus, user }: UserDetailPageProps) {
  const nextStatus = user.status === "active" ? "disabled" : "active";

  return (
    <>
      <PageHeader
        description="Review account state, access, and sign-in readiness."
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Root" },
              { label: "Users", onClick: onBack },
              { current: true, label: user.username }
            ]}
            label="User location"
          />
        }
        title={user.username}
      />
      <div className={styles.detailWorkspace}>
        <section className={`${styles.identityPanel} polished-panel polished-panel--accent-top`} aria-label="User identity">
          <div className={styles.detailHeader}>
            <div>
              <p className="eyebrow">Account</p>
              <h2>Summary</h2>
              <p>{user.lastSeen}</p>
            </div>
            <ResourceStatus tone={user.status}>{user.status === "active" ? "Active" : "Disabled"}</ResourceStatus>
          </div>
          <div className={styles.detailFacts}>
            <DetailFact label="Role" value={user.role} tone={roleTone(user.role)} />
            <DetailFact label="Provider" value="Local" />
            <DetailFact label="Account ID" value={user.id} />
          </div>
        </section>

        <section className={`${styles.detailPanel} polished-panel polished-panel--accent-top`} aria-labelledby="user-access-title">
          <div className={styles.detailCopy}>
            <p className="eyebrow">Access</p>
            <h2 id="user-access-title">Role Bindings</h2>
            <p>
              Role bindings are not exposed by the live users API yet. Accounts loaded from the API display as unassigned until that
              permission surface is wired in.
            </p>
          </div>
          <div className={styles.detailActions}>
            <Button onClick={() => onUpdateUserStatus(user.id, nextStatus)} type="button" variant="quiet">
              {user.status === "active" ? "Disable" : "Activate"}
            </Button>
            <Button disabled={user.username === "admin"} onClick={() => onRemoveUser(user)} type="button" variant="danger">
              Remove User
            </Button>
          </div>
        </section>
      </div>
      {children}
    </>
  );
}

function DetailFact({ label, tone, value }: { label: string; tone?: ReturnType<typeof roleTone>; value: string }) {
  return (
    <div className={styles.detailFact}>
      <span>{label}</span>
      {tone ? <ResourceStatus tone={tone}>{value}</ResourceStatus> : <strong>{value}</strong>}
    </div>
  );
}
