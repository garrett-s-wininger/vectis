import type { Meta, StoryObj } from "@storybook/react-vite";
import { FormError } from "./FormError";

const meta = {
  title: "Components/Feedback/FormError",
  component: FormError,
  args: {
    message: "Invalid username or password."
  }
} satisfies Meta<typeof FormError>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const PageAlert: Story = {
  args: {
    message: "Unable to trigger job. The saved definition could not be enqueued.",
    title: "Action Failed"
  },
  decorators: [
    (Story) => (
      <div className="app-alert-rail">
        <Story />
      </div>
    )
  ]
};
