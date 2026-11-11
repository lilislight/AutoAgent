import { memo } from 'react';
import { AgentDisclosure } from './AgentDisclosure.js';
import { AgentIcon } from './AgentIcon.js';
import { ToolArgumentsPreview, ToolResultPreview, ToolPreviewNotice } from './ToolPayloadPreview.js';
import { previewToolStrings } from './toolPreview.js';
import { presentTool, toolGroupSummary, toolStateLabel } from './toolPresentation.js';
import type { Interaction, ToolSnapshot } from './types.js';

export interface ConversationTool { id: string; tool?: ToolSnapshot; interaction?: Interaction }

function QuestionRecord({ interaction }: { interaction: Interaction }) {
  const questions = interaction.request.questions as unknown as { id: string; text: string; options?: { id: string; label: string }[] }[];
  const response = interaction.resolution?.response as unknown as { answers?: { question_id: string; text: string | null; status: string; selected_option_ids: string[] }[] } | null;
  const records = (questions ?? []).map(question => {
    const answer = response?.answers?.find(item => item.question_id === question.id);
    return { id: question.id, question: question.text, answer: answer?.status === 'skipped' ? 'Skipped' : answer ? [answer.text, ...(question.options ?? []).filter(option => answer.selected_option_ids.includes(option.id)).map(option => option.label)].filter(Boolean).join(' · ') : interaction.resolution?.outcome ?? '' };
  });
  const preview = previewToolStrings(records.flatMap(record => [record.question, record.answer]));
  return <>{preview.truncated ? <ToolPreviewNotice name="ask_user-answers.json" value={() => JSON.stringify(records, null, 2)} /> : null}
    <div className="agent-tool-preview" tabIndex={0} role="region" aria-label="Question answers preview">{records.map((record, index) => preview.texts[index * 2] || preview.texts[index * 2 + 1] ? <div key={record.id}><div className="agent-label">{preview.texts[index * 2]}</div><p>{preview.texts[index * 2 + 1]}</p></div> : null)}</div></>;
}
const ToolRow = memo(function ToolRow({ tool, interaction, lifecycle }: { tool?: ToolSnapshot; interaction?: Interaction; lifecycle?: string }) {
  const presentation = tool ? presentTool(tool) : interaction?.type === 'question' ? { title: interaction.state === 'resolved' ? 'Your answers' : 'Question for you', icon: 'message' as const } : { title: 'Tool call', icon: 'wrench' as const };
  const status = !tool && interaction?.type === 'question' ? interaction.state === 'resolved' ? undefined : 'Waiting' : toolStateLabel(tool, lifecycle);
  return <AgentDisclosure className={`agent-tool-row is-${tool?.state ?? 'unknown'}`} label={presentation.title}
    tooltip={presentation.title} icon={<AgentIcon name={presentation.icon} />} status={status}>
    {() => <div className="agent-tool-detail">
      {interaction?.type === 'question' && interaction.state === 'resolved' ? <QuestionRecord interaction={interaction} /> : <>
        {tool ? <><div className="agent-label">Arguments</div><ToolArgumentsPreview arguments={tool.arguments} name={tool.name} /></> : null}
        {tool?.result ? <><div className="agent-label">Result</div><ToolResultPreview content={tool.result.content} structured={tool.result.structured_content} name={tool.name} /></>
          : <p className="agent-muted">{status === 'Stopped' || status === 'Interrupted' ? 'Execution ended without a saved tool result.' : status === 'No saved result' ? 'No saved result is available.' : tool?.state === 'waiting' ? 'Waiting for input, an Agent update, or the deadline.' : 'The result will appear here when the call completes.'}</p>}
      </>}
    </div>}
  </AgentDisclosure>;
});

export function ConversationTools({ items, lifecycle }: { items: ConversationTool[]; lifecycle?: string }) {
  const tools = items.map(item => item.tool ?? { name: item.interaction?.type === 'question' ? 'ask_user' : 'tool_call', arguments: '{}', state: item.interaction?.state === 'resolved' ? 'completed' as const : 'waiting' as const });
  const labels = items.map(item => toolStateLabel(item.tool, lifecycle));
  const priority = ['Failed', 'Denied', 'Interrupted', 'Stopped', 'Waiting', 'Paused', 'Running', 'Queued'];
  const status = priority.filter(label => labels.includes(label)).map(label => `${labels.filter(value => value === label).length} ${label.toLowerCase()}`).join(' · ');
  const summary = toolGroupSummary(tools);
  return <AgentDisclosure className="agent-tool-group" label={summary} tooltip={summary} icon={<AgentIcon name="wrench" />} status={status || undefined}>
    <div className="agent-tool-list" tabIndex={0} role="region" aria-label="Tool calls">{items.map(item => <ToolRow key={item.id} tool={item.tool} interaction={item.interaction} lifecycle={lifecycle} />)}</div>
  </AgentDisclosure>;
}
