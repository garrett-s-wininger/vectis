/** @jsxRuntime classic */
/** Render schema content without Swagger's code-like braces and primitive toggles. */
export function schemaPlugin(system) {
  // NOTE(garrett): Swagger bundles its own React version; use it for every plugin element.
  const React = system.React;

  return {
    wrapComponents: {
      Models: () =>
        function Models() {
          const document = system.specSelectors.specJson().toJS();
          const schemas = Object.entries(document.components?.schemas ?? {});
          if (!schemas.length) return null;

          return (
            <section id="api-schemas" className="models vectis-schemas" aria-label="Schemas">
              <h2>Schemas</h2>
              {schemas.map(([name, schema]) => (
                <div className="model-container" data-name={name} key={name}>
                  <Schema name={name} schema={schema} document={document} />
                </div>
              ))}
            </section>
          );
        },
      Model: () =>
        function Model({ schema, name }) {
          const document = system.specSelectors.specJson().toJS();
          return React.createElement(Schema, { schema: schema?.toJS?.() ?? schema ?? {}, name, document });
        }
    }
  };

  function resolve(schema, document) {
    const reference = schema.$ref;
    if (!reference?.startsWith("#/")) return schema;

    const resolved = reference
      .slice(2)
      .split("/")
      .reduce((value, key) => value?.[key.replaceAll("~1", "/").replaceAll("~0", "~")], document);

    return resolved ? { ...resolved, ...schema, $ref: undefined } : schema;
  }

  function typeLabel(schema, document) {
    if (schema.$ref) return schema.$ref.split("/").at(-1);
    if (schema.type === "array") return `${typeLabel(schema.items ?? {}, document)}[]`;

    return (
      schema.type ??
      (schema.properties ? "object" : schema.oneOf || schema.anyOf || schema.allOf ? "composite" : "any value")
    );
  }

  function Schema({ schema: input, document, name, depth = 0 }) {
    const schema = resolve(input, document);
    const properties = Object.entries(schema.properties ?? {});
    const alternatives = schema.oneOf ?? schema.anyOf ?? schema.allOf;
    const items = schema.type === "array" ? resolve(schema.items ?? {}, document) : null;
    const structuredItems = items && (items.properties || items.oneOf || items.anyOf || items.allOf);
    const structured = properties.length > 0 || alternatives?.length > 0 || structuredItems;
    const label = typeLabel(input, document);
    const heading = (
      <>
        {name ? <span className="vectis-schema-name">{name}</span> : null}
        <span className="vectis-schema-type">{label}</span>
      </>
    );

    const description = (
      <>
        {schema.description ? <p>{schema.description}</p> : null}
        {schema.enum ? <p>Allowed values: {schema.enum.map((value) => JSON.stringify(value)).join(", ")}</p> : null}
        {schema.format ? <p>Format: {schema.format}</p> : null}
        {schema.nullable ? <p>May be null.</p> : null}
        {schema.minLength != null ? <p>Minimum length: {schema.minLength}</p> : null}
        {schema.type === "object" && !structured ? (
          <p>
            {schema.additionalProperties === false ? "Object with no properties." : "Object with arbitrary properties."}
          </p>
        ) : null}
      </>
    );

    if (!structured || depth >= 8)
      return (
        <div className="vectis-schema-leaf">
          {heading}
          {description}
        </div>
      );

    return (
      <details className="vectis-schema">
        <summary>{heading}</summary>
        <div className="vectis-schema-content">
          {description}
          {properties.length > 0 ? (
            <table className="model vectis-schema-table">
              <thead>
                <tr>
                  <th scope="col">Property</th>
                  <th scope="col">Type and description</th>
                </tr>
              </thead>
              <tbody>
                {properties.map(([property, value]) => (
                  <tr key={property}>
                    <th scope="row">
                      <code>{property}</code>
                      {schema.required?.includes(property) ? (
                        <span className="vectis-schema-required">Required</span>
                      ) : null}
                    </th>
                    <td>
                      <Schema schema={value} document={document} depth={depth + 1} />
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : null}
          {schema.additionalProperties === false && properties.length > 0 ? (
            <p className="vectis-schema-note">Additional properties are not allowed.</p>
          ) : null}
          {schema.additionalProperties && typeof schema.additionalProperties === "object" ? (
            <Schema
              name="Additional properties"
              schema={schema.additionalProperties}
              document={document}
              depth={depth + 1}
            />
          ) : null}
          {alternatives ? (
            <div className="vectis-schema-alternatives">
              <p>{schema.oneOf ? "Exactly one of:" : schema.anyOf ? "Any of:" : "All of:"}</p>
              {alternatives.map((value, index) => (
                <Schema key={index} schema={value} document={document} depth={depth + 1} />
              ))}
            </div>
          ) : null}
          {structuredItems ? <Schema name="Items" schema={schema.items} document={document} depth={depth + 1} /> : null}
        </div>
      </details>
    );
  }
}
