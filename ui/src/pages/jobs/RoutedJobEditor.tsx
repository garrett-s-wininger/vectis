import { useState } from "react";
import type { NewJob, UpdateJob } from "../../domain/console";
import { JobEditor } from "./JobEditor";
import type { JobEditorMode, JobFormValues } from "./JobEditorModel";

type RoutedJobEditorProps = {
  initialValues: JobFormValues;
  mode: JobEditorMode;
  namespacePath: string;
  onCancel: () => void;
  onCreateJob: (input: NewJob) => Promise<void> | void;
  onUpdateJob: (jobID: string, input: UpdateJob) => Promise<void> | void;
};

export function RoutedJobEditor({
  initialValues,
  mode,
  namespacePath,
  onCancel,
  onCreateJob,
  onUpdateJob
}: RoutedJobEditorProps) {
  const [values, setValues] = useState<JobFormValues>(initialValues);
  const [formError, setFormError] = useState("");

  return (
    <JobEditor
      error={formError}
      mode={mode}
      namespacePath={namespacePath}
      onCancel={onCancel}
      onCreateJob={onCreateJob}
      onError={setFormError}
      onUpdateJob={onUpdateJob}
      onValuesChange={setValues}
      values={values}
    />
  );
}
