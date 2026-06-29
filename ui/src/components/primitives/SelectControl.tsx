import type { ChangeEvent, KeyboardEvent, MouseEvent, SelectHTMLAttributes } from "react";
import { useRef, useState } from "react";
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
  const detailsRef = useRef<HTMLDetailsElement>(null);
  const summaryRef = useRef<HTMLElement>(null);
  const optionRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const isControlled = value !== undefined;
  const [isOpen, setIsOpen] = useState(false);
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
    closeDropdown();
    closeParentDropdown(event.currentTarget);
  }

  function handleSummaryKeyDown(event: KeyboardEvent<HTMLElement>) {
    if (disabled) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      openDropdown(focusableOptionIndex(selectedValue, "next"));
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      openDropdown(focusableOptionIndex(selectedValue, "previous"));
    }

    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();

      if (isOpen) {
        closeDropdown();
      } else {
        openDropdown(focusableOptionIndex(selectedValue, "current"));
      }
    }
  }

  function handleMenuKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeDropdown({ focusSummary: true });
      return;
    }

    if (event.key === "Home") {
      event.preventDefault();
      focusOption(focusableOptionIndex("", "first"));
      return;
    }

    if (event.key === "End") {
      event.preventDefault();
      focusOption(focusableOptionIndex("", "last"));
      return;
    }

    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") {
      return;
    }

    event.preventDefault();

    const activeElement = event.currentTarget.ownerDocument.activeElement;
    const activeIndex = optionRefs.current.findIndex((option) => option === activeElement);
    const nextIndex = focusableOptionIndex(
      options[activeIndex]?.value ?? selectedValue,
      event.key === "ArrowDown" ? "next" : "previous"
    );

    focusOption(nextIndex);
  }

  function openDropdown(optionIndex: number) {
    if (!detailsRef.current || disabled) {
      return;
    }

    closeOtherDropdownsFromTrigger(detailsRef.current);
    detailsRef.current.open = true;
    setIsOpen(true);
    focusOption(optionIndex);
  }

  function closeDropdown({ focusSummary = false } = {}) {
    if (detailsRef.current) {
      detailsRef.current.open = false;
    }

    setIsOpen(false);

    if (focusSummary) {
      summaryRef.current?.focus();
    }
  }

  function toggleDropdown() {
    if (isOpen) {
      closeDropdown();
      return;
    }

    openDropdown(-1);
  }

  function focusOption(index: number) {
    if (index < 0) {
      return;
    }

    window.requestAnimationFrame(() => optionRefs.current[index]?.focus());
  }

  function focusableOptionIndex(anchorValue: string, direction: "current" | "first" | "last" | "next" | "previous") {
    const enabledIndexes = options.flatMap((option, index) => (option.disabled ? [] : [index]));

    if (enabledIndexes.length === 0) {
      return -1;
    }

    if (direction === "first") {
      return enabledIndexes[0];
    }

    if (direction === "last") {
      return enabledIndexes[enabledIndexes.length - 1];
    }

    const selectedIndex = options.findIndex((option) => option.value === anchorValue && !option.disabled);

    if (direction === "current") {
      return selectedIndex >= 0 ? selectedIndex : enabledIndexes[0];
    }

    const currentEnabledPosition = enabledIndexes.findIndex((index) => index === selectedIndex);

    if (currentEnabledPosition < 0) {
      return direction === "next" ? enabledIndexes[0] : enabledIndexes[enabledIndexes.length - 1];
    }

    const offset = direction === "next" ? 1 : -1;
    const nextPosition = (currentEnabledPosition + offset + enabledIndexes.length) % enabledIndexes.length;

    return enabledIndexes[nextPosition];
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
        onToggle={(event) => {
          setIsOpen(event.currentTarget.open);
          closeOtherDropdowns(event.currentTarget);
        }}
        ref={detailsRef}
      >
        <summary
          aria-describedby={ariaDescribedBy}
          aria-disabled={disabled ? true : undefined}
          aria-expanded={isOpen}
          aria-haspopup="listbox"
          aria-label={`${summaryLabel}: ${selectedLabel}`}
          className={styles.summary}
          onClick={(event) => {
            event.preventDefault();

            if (disabled) {
              return;
            }

            toggleDropdown();
          }}
          onKeyDown={handleSummaryKeyDown}
          ref={summaryRef}
        >
          <span className={styles.value}>{selectedLabel}</span>
        </summary>
        <div className={styles.menu} hidden={!isOpen} onKeyDown={handleMenuKeyDown} role="listbox">
          {options.map((option, index) => (
            <button
              aria-current={option.value === selectedValue ? "true" : undefined}
              aria-selected={option.value === selectedValue}
              className={styles.menuItem}
              disabled={option.disabled}
              key={option.value}
              onClick={(event) => handleOptionClick(option, event)}
              ref={(element) => {
                optionRefs.current[index] = element;
              }}
              role="option"
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
