import { SelectField } from "./primitives/SelectField";
import { setThemePreference, useThemePreference, type ThemePreference as Preference } from "../theme";

export function ThemePreference({ label = "Theme" }: { label?: string }) {
  const preference = useThemePreference();
  return (
    <SelectField
      label={label}
      value={preference}
      onChange={(event) => setThemePreference(event.target.value as Preference)}
      options={[
        { value: "system", label: "System" },
        { value: "light", label: "Light" },
        { value: "dark", label: "Dark" }
      ]}
    />
  );
}
