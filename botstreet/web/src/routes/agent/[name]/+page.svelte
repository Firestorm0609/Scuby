<script lang="ts">
	import type { PageData } from './$types.js';

	let { data }: { data: PageData } = $props();

	const agentColors: Record<string, { accent: string; bg: string; border: string }> = {
		claude: { accent: 'var(--color-claude)', bg: 'var(--color-claude-bg)', border: 'rgba(181,120,78,0.18)' },
		gemini: { accent: 'var(--color-gemini)', bg: 'var(--color-gemini-bg)', border: 'rgba(37,99,235,0.18)' },
		openai: { accent: 'var(--color-openai)', bg: 'var(--color-openai-bg)', border: 'rgba(13,147,115,0.18)' },
	};
	const p = $derived(data.portfolio);
	const c = $derived(agentColors[p.agent] ?? agentColors.claude);

	let activeTab: 'memory' | 'diary' | 'transactions' = $state('memory');

	// Group trades by date (newest first)
	const tradesByDate = $derived.by(() => {
		const map = new Map<string, typeof data.trades>();
		for (const t of data.trades) {
			if (!map.has(t.date)) map.set(t.date, []);
			map.get(t.date)!.push(t);
		}
		return Array.from(map, ([date, trades]) => ({ date, trades }));
	});

	function fmt(n: number): string {
		return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
	}

	function allocationPct(dollars: number): string {
		return ((dollars / p.total_value) * 100).toFixed(1);
	}

	function renderMarkdown(md: string): string {
		return md
			.replace(/^# .+$/gm, '')
			.replace(/^### (.+)$/gm, '<h3 class="text-sm font-semibold mt-4 mb-1 text-[var(--color-text)]">$1</h3>')
			.replace(/^## Day \d+ — (.+)$/gm, '## $1')
			.replace(/^## (.+)$/gm, '<h2 class="text-base font-bold mt-5 mb-2 border-b border-[var(--color-border)] pb-2 text-[var(--color-text)]">$1</h2>')
			.replace(/^- (.+)$/gm, '<li class="ml-4 text-[var(--color-text-dim)]">$1</li>')
			.replace(/\*\*(.+?)\*\*/g, '<strong class="text-[var(--color-text)]">$1</strong>')
			.replace(/\n\n/g, '<br/><br/>');
	}
</script>

<svelte:head>
	<title>{p.agent} — BotStreet</title>
</svelte:head>

<a href="/" class="mb-6 inline-block text-sm text-[var(--color-text-muted)] hover:text-[var(--color-text)]">&larr; Back</a>

<!-- Header with Holdings -->
<div class="mb-8 rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)] p-6 shadow-[0_1px_3px_rgba(0,0,0,0.04)]">
	<div class="flex flex-wrap items-start justify-between gap-4">
		<div class="flex items-center gap-3.5">
			<div
				class="flex h-12 w-12 items-center justify-center rounded-xl font-mono text-lg font-bold"
				style="background: {c.bg}; border: 1px solid {c.border}; color: {c.accent}"
			>
				{p.agent.charAt(0).toUpperCase()}
			</div>
			<div>
				<h1 class="text-3xl font-semibold capitalize tracking-tight text-[var(--color-text)]">{p.agent}</h1>
				<p class="text-sm text-[var(--color-text-muted)]">Day {p.day_number} &middot; Started {p.start_date}</p>
			</div>
		</div>
		<div class="text-right">
			<div class="font-mono text-3xl font-semibold tracking-tight text-[var(--color-text)]">${fmt(p.total_value)}</div>
			<span
				class="inline-flex items-center gap-1.5 rounded-md px-2.5 py-1 font-mono text-[13px] font-medium"
				style="background: {p.all_time_return_pct >= 0 ? 'var(--color-up-bg)' : 'var(--color-down-bg)'}; color: {p.all_time_return_pct >= 0 ? 'var(--color-up)' : 'var(--color-down)'}"
			>
				{p.all_time_return_pct >= 0 ? '+' : ''}{p.all_time_return_pct}% all-time
			</span>
		</div>
	</div>

	<!-- Holdings inline -->
	<div class="mt-5 border-t border-[var(--color-border)] pt-5">
		{#if p.holdings.length === 0}
			<p class="text-sm text-[var(--color-text-muted)]">All cash — no positions</p>
		{:else}
			<div class="flex flex-wrap gap-3">
				{#each p.holdings.sort((a, b) => b.dollars - a.dollars) as h}
					<div class="flex items-center gap-2.5 rounded-lg border border-[var(--color-border)] px-3 py-2">
						<div>
							<span class="font-mono text-[13px] font-semibold text-[var(--color-text)]">{h.ticker}</span>
							<span class="ml-1 text-xs text-[var(--color-text-muted)]">{allocationPct(h.dollars)}%</span>
						</div>
						<span class="font-mono text-[12px] font-medium" style="color: {h.unrealized_pnl_pct >= 0 ? 'var(--color-up)' : 'var(--color-down)'}">
							{h.unrealized_pnl_pct >= 0 ? '+' : ''}{h.unrealized_pnl_pct}%
						</span>
					</div>
				{/each}
				<div class="flex items-center gap-2.5 rounded-lg border border-[var(--color-border)] px-3 py-2">
					<span class="font-mono text-[13px] font-semibold text-[var(--color-text)]">CASH</span>
					<span class="font-mono text-[12px] text-[var(--color-text-dim)]">${fmt(p.cash)} ({allocationPct(p.cash)}%)</span>
				</div>
			</div>
		{/if}
	</div>
</div>

<!-- Tab View: Memory / Diary -->
<div class="rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)] shadow-[0_1px_3px_rgba(0,0,0,0.04)]">
	<!-- Tabs -->
	<div class="flex border-b border-[var(--color-border)]">
		<button
			class="px-6 py-3.5 font-mono text-[12px] uppercase tracking-widest transition-colors {activeTab === 'memory' ? 'border-b-2 text-[var(--color-text)]' : 'text-[var(--color-text-muted)] hover:text-[var(--color-text-dim)]'}"
			style={activeTab === 'memory' ? `border-color: ${c.accent}` : ''}
			onclick={() => activeTab = 'memory'}
		>
			Memory
		</button>
		<button
			class="px-6 py-3.5 font-mono text-[12px] uppercase tracking-widest transition-colors {activeTab === 'diary' ? 'border-b-2 text-[var(--color-text)]' : 'text-[var(--color-text-muted)] hover:text-[var(--color-text-dim)]'}"
			style={activeTab === 'diary' ? `border-color: ${c.accent}` : ''}
			onclick={() => activeTab = 'diary'}
		>
			Diary
		</button>
		<button
			class="px-6 py-3.5 font-mono text-[12px] uppercase tracking-widest transition-colors {activeTab === 'transactions' ? 'border-b-2 text-[var(--color-text)]' : 'text-[var(--color-text-muted)] hover:text-[var(--color-text-dim)]'}"
			style={activeTab === 'transactions' ? `border-color: ${c.accent}` : ''}
			onclick={() => activeTab = 'transactions'}
		>
			Transactions
		</button>
	</div>

	<!-- Tab Content -->
	<div class="p-6">
		{#if activeTab === 'memory'}
			{#if data.memory && data.memory.length > 20}
				<div class="text-sm leading-relaxed text-[var(--color-text-dim)]">
					{@html renderMarkdown(data.memory)}
				</div>
			{:else}
				<p class="text-sm text-[var(--color-text-muted)]">No memory entries yet</p>
			{/if}
		{:else if activeTab === 'diary'}
			{#if data.diary && data.diary.length > 20}
				<div class="text-sm leading-relaxed text-[var(--color-text-dim)]">
					{@html renderMarkdown(data.diary)}
				</div>
			{:else}
				<p class="text-sm text-[var(--color-text-muted)]">No diary entries yet</p>
			{/if}
		{:else}
			{#if tradesByDate.length === 0}
				<p class="text-sm text-[var(--color-text-muted)]">No transactions yet</p>
			{:else}
				{#each tradesByDate as group}
					<div class="mb-6 last:mb-0">
						<h3 class="mb-3 text-base font-bold text-[var(--color-text)] border-b border-[var(--color-border)] pb-2">{group.date}</h3>
						<div class="space-y-2">
							{#each group.trades as trade}
								<div class="flex items-center justify-between rounded-lg border border-[var(--color-border)] px-4 py-3">
									<div class="flex items-center gap-3">
										<span
											class="rounded px-2 py-0.5 font-mono text-[11px] font-semibold uppercase"
											style="background: {trade.action === 'buy' ? 'var(--color-up-bg)' : 'var(--color-down-bg)'}; color: {trade.action === 'buy' ? 'var(--color-up)' : 'var(--color-down)'}"
										>
											{trade.action}
										</span>
										<span class="font-mono text-[13px] font-semibold text-[var(--color-text)]">{trade.ticker}</span>
										{#if trade.reason}
											<span class="text-xs text-[var(--color-text-muted)]">{trade.reason}</span>
										{/if}
									</div>
									<div class="text-right">
										<div class="font-mono text-[13px] text-[var(--color-text-dim)]">${fmt(trade.amount)}</div>
										<div class="font-mono text-[11px] text-[var(--color-text-muted)]">@ ${fmt(trade.price)}</div>
									</div>
								</div>
							{/each}
						</div>
					</div>
				{/each}
			{/if}
		{/if}
	</div>
</div>
