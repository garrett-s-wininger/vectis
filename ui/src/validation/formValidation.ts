export type ValidationResult = { valid: true } | { message: string; valid: false };

type CronRange = {
  label: string;
  max: number;
  min: number;
};

const cronFieldRanges: CronRange[] = [
  { label: "Minute", max: 59, min: 0 },
  { label: "Hour", max: 23, min: 0 },
  { label: "Day", max: 31, min: 1 },
  { label: "Month", max: 12, min: 1 },
  { label: "Weekday", max: 7, min: 0 }
];

export function valid(): ValidationResult {
  return { valid: true };
}

export function invalid(message: string): ValidationResult {
  return { message, valid: false };
}

export function required(value: string, message = "Enter a value."): ValidationResult {
  return value.trim() ? valid() : invalid(message);
}

export function jsonObject(value: string): ValidationResult {
  try {
    const parsed = JSON.parse(value) as unknown;

    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
      return invalid("Definition must be a JSON object.");
    }

    return valid();
  } catch {
    return invalid("Definition must be valid JSON.");
  }
}

export function cronSpec(value: string): ValidationResult {
  const spec = value.trim();

  if (!spec) {
    return invalid("Enter a cron spec.");
  }

  const fields = spec.split(/\s+/);

  if (fields.length !== 5) {
    return invalid("Use five cron fields: minute hour day month weekday.");
  }

  for (const [index, field] of fields.entries()) {
    const result = validateCronField(field, cronFieldRanges[index]);

    if (!result.valid) {
      return result;
    }
  }

  return valid();
}

function validateCronField(field: string, range: CronRange): ValidationResult {
  if (!field) {
    return invalid(`${range.label} is required.`);
  }

  for (const part of field.split(",")) {
    const result = validateCronPart(part, range);

    if (!result.valid) {
      return result;
    }
  }

  return valid();
}

function validateCronPart(part: string, range: CronRange): ValidationResult {
  if (!part) {
    return invalid(`${range.label} contains an empty list item.`);
  }

  const [base, step, extra] = part.split("/");

  if (extra !== undefined) {
    return invalid(`${range.label} has too many step separators.`);
  }

  if (step !== undefined) {
    const stepValue = Number(step);

    if (!Number.isInteger(stepValue) || stepValue < 1) {
      return invalid(`${range.label} step must be at least 1.`);
    }
  }

  if (base === "*") {
    return valid();
  }

  const [start, end, extraRange] = base.split("-");

  if (extraRange !== undefined) {
    return invalid(`${range.label} has too many range separators.`);
  }

  const startResult = validateCronNumber(start, range);

  if (!startResult.valid || end === undefined) {
    return startResult;
  }

  const endResult = validateCronNumber(end, range);

  if (!endResult.valid) {
    return endResult;
  }

  if (Number(start) > Number(end)) {
    return invalid(`${range.label} range must start before it ends.`);
  }

  return valid();
}

function validateCronNumber(value: string, range: CronRange): ValidationResult {
  const parsed = Number(value);

  if (!Number.isInteger(parsed)) {
    return invalid(`${range.label} must be a number, *, range, list, or step.`);
  }

  if (parsed < range.min || parsed > range.max) {
    return invalid(`${range.label} must be between ${range.min} and ${range.max}.`);
  }

  return valid();
}
