import styles from "./ErrorAlert.module.css";

type ErrorAlertProps = {
  message?: string;
  title?: string;
};

export function ErrorAlert({ message, title }: ErrorAlertProps) {
  if (!message) {
    return null;
  }

  return (
    <div className={styles.root} role="alert">
      {title ? <p className={styles.title}>{title}</p> : null}
      <p className={styles.message}>{message}</p>
    </div>
  );
}
