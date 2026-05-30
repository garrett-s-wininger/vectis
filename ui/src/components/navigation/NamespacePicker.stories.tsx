import type { Meta, StoryObj } from "@storybook/react-vite";
import { NamespacePicker } from "./NamespacePicker";

const meta = {
  title: "Components/Navigation/NamespacePicker",
  component: NamespacePicker,
  args: {
    namespaces: [
      {
        id: 1,
        name: "/",
        path: "/",
        breakInheritance: false,
        role: "Admin"
      },
      {
        id: 2,
        name: "team-a",
        parentID: 1,
        path: "/team-a",
        breakInheritance: false,
        role: "Operator"
      }
    ],
    onChange: () => undefined,
    value: "/"
  }
} satisfies Meta<typeof NamespacePicker>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
