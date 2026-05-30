import type { Meta, StoryObj } from "@storybook/react-vite";
import { storyNamespaces } from "../../mocks/storyFixtures";
import { NamespacePicker } from "./NamespacePicker";

const meta = {
  title: "Components/Navigation/NamespacePicker",
  component: NamespacePicker,
  args: {
    namespaces: storyNamespaces,
    onChange: () => undefined,
    value: "/"
  }
} satisfies Meta<typeof NamespacePicker>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
