import styles from "./ToggleField.module.css";

type ToggleFieldProps = {
  checked: boolean;
  hideLabel?: boolean;
  label: string;
  name: string;
  offText: string;
  onChange: (checked: boolean) => void;
  onText: string;
};

export function ToggleField({ checked, hideLabel, label, name, offText, onChange, onText }: ToggleFieldProps) {
  return (
    <label className={`${styles.root} ${hideLabel ? styles.labelHidden : ""}`}>
      {hideLabel ? null : <span>{label}</span>}
      <span className={styles.control}>
        <input
          aria-label={label}
          checked={checked}
          name={name}
          onChange={(event) => onChange(event.target.checked)}
          type="checkbox"
        />
        <span aria-hidden="true" className={styles.track}>
          <span className={styles.thumb} />
        </span>
        <span className={styles.value}>{checked ? onText : offText}</span>
      </span>
    </label>
  );
}
