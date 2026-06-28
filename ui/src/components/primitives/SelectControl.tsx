import type { ChangeEvent, MouseEvent, SelectHTMLAttributes } from "react";
import { useState } from "react";
import {
  closeOtherDropdowns,
  closeOtherDropdownsFromTrigger,
  closeParentDropdown,
  coordinatedDropdownProps
} from "../navigation/dropdownCoordination";
import styles from "./SelectControl.module.css";

export type SelectControlOption = {
  disabled?: boolean;
  label: string;
  value: string;
};

type SelectControlProps = Omit<SelectHTMLAttributes<HTMLSelectElement>, "children" | "multiple" | "size"> & {
  invalid?: boolean;
  options: SelectControlOption[];
  summaryLabel: string;
};

export function SelectControl({
  "aria-describedby": ariaDescribedBy,
  "aria-invalid": ariaInvalid,
  className,
  defaultValue,
  disabled,
  invalid,
  onChange,
  options,
  summaryLabel,
  value,
  ...nativeProps
}: SelectControlProps) {
  const isControlled = value !== undefined;
  const [internalValue, setInternalValue] = useState(() =>
    normalizeValue(defaultValue ?? options.find((option) => !option.disabled)?.value ?? options[0]?.value ?? "")
  );

  const selectedValue = normalizeValue(isControlled ? value : internalValue);
  const selectedOption =
    options.find((option) => option.value === selectedValue) ??
    options.find((option) => !option.disabled) ??
    options[0];

  const selectedLabel = selectedOption?.label ?? "";

  function handleNativeChange(event: ChangeEvent<HTMLSelectElement>) {
    if (!isControlled) {
      setInternalValue(event.target.value);
    }

    onChange?.(event);
  }

  function handleOptionClick(option: SelectControlOption, event: MouseEvent<HTMLButtonElement>) {
    if (disabled || option.disabled) {
      return;
    }

    if (!isControlled) {
      setInternalValue(option.value);
    }

    onChange?.(selectChangeEvent(option.value, nativeProps.name));
    closeParentDropdown(event.currentTarget);
  }

  return (
    <span className={`${styles.container} ${className ?? ""}`}>
      <select
        {...nativeProps}
        aria-describedby={ariaDescribedBy}
        aria-invalid={invalid ? true : ariaInvalid}
        className={styles.nativeSelect}
        disabled={disabled}
        onChange={handleNativeChange}
        tabIndex={-1}
        value={selectedValue}
      >
        {options.map((option) => (
          <option disabled={option.disabled} key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
      <details
        {...coordinatedDropdownProps}
        className={styles.root}
        data-disabled={disabled ? "true" : undefined}
        data-invalid={invalid ? "true" : undefined}
        onToggle={(event) => closeOtherDropdowns(event.currentTarget)}
      >
        <summary
          aria-disabled={disabled ? true : undefined}
          aria-label={`${summaryLabel}: ${selectedLabel}`}
          className={styles.summary}
          onClick={(event) => {
            if (disabled) {
              event.preventDefault();
              return;
            }

            closeOtherDropdownsFromTrigger(event.currentTarget);
          }}
        >
          <span className={styles.value}>{selectedLabel}</span>
        </summary>
        <div className={styles.menu}>
          {options.map((option) => (
            <button
              aria-current={option.value === selectedValue ? "true" : undefined}
              className={styles.menuItem}
              disabled={option.disabled}
              key={option.value}
              onClick={(event) => handleOptionClick(option, event)}
              type="button"
            >
              {option.label}
            </button>
          ))}
        </div>
      </details>
    </span>
  );
}

function normalizeValue(value: SelectHTMLAttributes<HTMLSelectElement>["value"]) {
  if (Array.isArray(value)) {
    return String(value[0] ?? "");
  }

  return String(value ?? "");
}

function selectChangeEvent(value: string, name?: string) {
  const target = { name, value } as HTMLSelectElement;

  return {
    currentTarget: target,
    target
  } as ChangeEvent<HTMLSelectElement>;
}
