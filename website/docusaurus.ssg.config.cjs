const config = require("./docusaurus.config.js");

module.exports = {
  ...config,
  future: {
    ...config.future,
    v4: {
      ...config.future?.v4,
      removeLegacyPostBuildHeadAttribute: true
    },
    faster: {
      ...config.future?.faster,
      ssgWorkerThreads: true
    }
  }
};
