import styles from "./FormError.module.css";

type FormErrorProps = {
  message?: string;
};

export function FormError({ message }: FormErrorProps) {
  if (!message) {
    return null;
  }

  return (
    <p className={styles.root} role="alert">
      {message}
    </p>
  );
}
