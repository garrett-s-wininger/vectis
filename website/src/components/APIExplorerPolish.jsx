/** @jsxRuntime classic */
/** Keep the read-only explorer's controls informative without suggesting live requests. */
export function explorerPlugin(system) {
  const React = system.React;
  return {
    wrapComponents: {
      contentType: (Original) =>
        function ContentType(props) {
          const only = props.contentTypes?.size === 1 ? props.contentTypes.first() : null;

          // NOTE(garrett): Preserve Swagger's media selection even when there is no choice to display.
          React.useEffect(() => {
            if (only && props.value !== only) props.onChange?.(only);
          }, [only, props.value, props.onChange]);

          return only ? <span className="vectis-media-type">{only}</span> : React.createElement(Original, props);
        },
      authorizeOperationBtn: () =>
        function AuthenticationRequirement() {
          const Lock = system.getComponent("LockAuthOperationIcon", true);

          return (
            <span
              className="vectis-api-auth"
              role="img"
              aria-label="Bearer authentication required"
              title="Bearer authentication required"
            >
              <Lock aria-hidden="true" />
              <span aria-hidden="true">Auth</span>
            </span>
          );
        }
    }
  };
}
