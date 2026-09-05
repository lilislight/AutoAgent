interface Props {
  title: string;
  value: unknown;
  placeholder: string;
}

export function JsonInspector({ title, value, placeholder }: Props) {
  return (
    <section className="inspector-card">
      <header><span>{title}</span></header>
      {value === null || value === undefined ? (
        <div className="inspector-placeholder">{placeholder}</div>
      ) : (
        <pre>{JSON.stringify(value, null, 2)}</pre>
      )}
    </section>
  );
}
