import { addons } from "storybook/manager-api";

addons.setConfig({
  sidebar: {
    showRoots: true,
    collapsedRoots: []
  },
  toolbar: {
    copy: { hidden: true },
    eject: { hidden: true },
    fullscreen: { hidden: true },
    remount: { hidden: true },
    title: { hidden: true },
    zoom: { hidden: true }
  }
});
