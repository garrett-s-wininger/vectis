import { ErrorAlert } from "./ErrorAlert";

type FormErrorProps = {
  message?: string;
  title?: string;
};

export function FormError({ message, title }: FormErrorProps) {
  return <ErrorAlert message={message} title={title} />;
}
