import type { ChangeEvent, KeyboardEvent, MouseEvent, ReactNode, SelectHTMLAttributes } from "react";
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
  description?: string;
  label: string;
  value: string;
};

type SelectControlProps = Omit<SelectHTMLAttributes<HTMLSelectElement>, "children" | "multiple" | "size"> & {
  invalid?: boolean;
  leadingIcon?: ReactNode;
  options: SelectControlOption[];
  summaryLabel: string;
  searchable?: boolean;
};

export function SelectControl({
  "aria-describedby": ariaDescribedBy,
  "aria-invalid": ariaInvalid,
  className,
  defaultValue,
  disabled,
  invalid,
  leadingIcon,
  onChange,
  options,
  summaryLabel,
  searchable = false,
  value,
  ...nativeProps
}: SelectControlProps) {
  const detailsRef = useRef<HTMLDetailsElement>(null);
  const summaryRef = useRef<HTMLElement>(null);
  const optionRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const searchRef = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState("");
  const matches = (option: SelectControlOption) =>
    !searchable ||
    `${option.label} ${option.value} ${option.description ?? ""}`.toLowerCase().includes(query.trim().toLowerCase());

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
    closeDropdown({ focusSummary: searchable });
    closeParentDropdown(event.currentTarget);
  }

  function handleSummaryKeyDown(event: KeyboardEvent<HTMLElement>) {
    if (disabled) {
      return;
    }

    if (event.key === "ArrowDown") {
      event.preventDefault();
      openDropdown(searchable ? -1 : focusableOptionIndex(selectedValue, "next"));
    }

    if (event.key === "ArrowUp") {
      event.preventDefault();
      openDropdown(searchable ? -1 : focusableOptionIndex(selectedValue, "previous"));
    }

    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();

      if (isOpen) {
        closeDropdown();
      } else {
        openDropdown(searchable ? -1 : focusableOptionIndex(selectedValue, "current"));
      }
    }
  }

  function handleMenuKeyDown(event: KeyboardEvent<HTMLDivElement>) {
    if (event.target === searchRef.current && event.key !== "Escape") {
      if (event.key === "Enter") {
        event.preventDefault();
        const first = focusableOptionIndex("", "first");
        optionRefs.current[first]?.click();
      }

      if (event.key === "ArrowDown" || event.key === "ArrowUp") {
        event.preventDefault();
        focusOption(focusableOptionIndex("", event.key === "ArrowDown" ? "first" : "last"));
      }

      return;
    }

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

    if (searchable && optionIndex < 0) window.requestAnimationFrame(() => searchRef.current?.focus());
    else focusOption(optionIndex);
  }

  function closeDropdown({ focusSummary = false } = {}) {
    if (detailsRef.current) {
      detailsRef.current.open = false;
    }

    setIsOpen(false);
    setQuery("");

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
    const enabledIndexes = options.flatMap((option, index) => (option.disabled || !matches(option) ? [] : [index]));

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
          if (!event.currentTarget.open) setQuery("");
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
          {leadingIcon ? (
            <span className={styles.icon} aria-hidden="true">
              {leadingIcon}
            </span>
          ) : null}
          <span className={styles.value}>{selectedLabel}</span>
        </summary>
        <div className={styles.menu} hidden={!isOpen} onKeyDown={handleMenuKeyDown}>
          {searchable ? (
            <input
              ref={searchRef}
              className={styles.search}
              type="search"
              disabled={disabled}
              aria-label={`Search ${summaryLabel.toLowerCase()}`}
              placeholder="Search by name or provider…"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
            />
          ) : null}
          <div className={styles.optionList} role="listbox" aria-label={`${summaryLabel} options`}>
            {options.map((option, index) =>
              matches(option) ? (
                <button
                  aria-current={option.value === selectedValue ? "true" : undefined}
                  aria-selected={option.value === selectedValue}
                  className={styles.menuItem}
                  disabled={disabled || option.disabled}
                  key={option.value}
                  onClick={(event) => handleOptionClick(option, event)}
                  ref={(element) => {
                    optionRefs.current[index] = element;
                  }}
                  role="option"
                  type="button"
                >
                  <span className={styles.optionLabel}>{option.label}</span>
                  {option.description ? <small className={styles.optionDescription}>{option.description}</small> : null}
                </button>
              ) : null
            )}
            {!options.some(matches) ? (
              <div className={styles.emptyOption} role="option" aria-disabled="true" aria-selected="false">
                No matching options.
              </div>
            ) : null}
          </div>
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
