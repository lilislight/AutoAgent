interface Props {
  agent: boolean;
  agentHref: string;
  workflowHref: string;
  onChange: (agent: boolean) => void;
}

export function ApplicationSwitch({ agent, agentHref, workflowHref, onChange }: Props) {
  return <nav className="product-mode-switch" aria-label="Application">
    {([{ label: 'Agent', value: true, href: agentHref }, { label: 'Workflow', value: false, href: workflowHref }]).map(item => <a key={item.label} href={item.href} aria-current={agent === item.value ? 'page' : undefined} onClick={event => {
      if (event.button || event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
      event.preventDefault(); onChange(item.value);
    }}>{item.label}</a>)}
  </nav>;
}
