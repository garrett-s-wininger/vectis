import { ErrorAlert } from "../../components";

type ActionAlertRailProps = {
  message: string;
  title?: string;
};

export function ActionAlertRail({ message, title = "Action Failed" }: ActionAlertRailProps) {
  if (!message) {
    return null;
  }

  return (
    <div className="app-alert-rail">
      <ErrorAlert message={message} title={title} />
    </div>
  );
}
