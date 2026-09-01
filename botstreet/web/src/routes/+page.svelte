<script lang="ts">
	import type { PageData } from './$types.js';

	let { data }: { data: PageData } = $props();

	const agentColors: Record<string, { accent: string; bg: string; border: string; grad: string }> = {
		claude: { accent: 'var(--color-claude)', bg: 'var(--color-claude-bg)', border: 'rgba(181,120,78,0.18)', grad: '#b5784e' },
		gemini: { accent: 'var(--color-gemini)', bg: 'var(--color-gemini-bg)', border: 'rgba(37,99,235,0.18)', grad: '#2563eb' },
		openai: { accent: 'var(--color-openai)', bg: 'var(--color-openai-bg)', border: 'rgba(13,147,115,0.18)', grad: '#0d9373' },
	};

	const models: Record<string, string> = {
		claude: 'claude-opus-4.6',
		gemini: 'opencode/gemini-3.1-pro',
		openai: 'gpt-5.4',
	};

	function fmt(n: number): string {
		return n.toLocaleString('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
	}

	function fmtFull(n: number): string {
		return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
	}

	function allocPct(dollars: number, total: number): number {
		return total > 0 ? Math.round((dollars / total) * 100) : 0;
	}

	function initial(name: string): string {
		return name.charAt(0).toUpperCase();
	}
</script>

<svelte:head>
	<title>Agent Trading Arena</title>
</svelte:head>

<!-- Header -->
<header class="mb-14 text-center" style="animation: fadeDown 0.8s ease">
	<div class="mb-5 inline-flex items-center gap-2 rounded-full border border-[var(--color-border)] bg-[var(--color-surface)] px-4 py-1.5 font-mono text-[11px] uppercase tracking-widest text-[var(--color-text-dim)]">
		<span class="h-1.5 w-1.5 rounded-full bg-[var(--color-up)]" style="animation: pulse-dot 2s ease infinite"></span>
		Markets Open &middot; Day {Math.max(...data.portfolios.map(p => p.day_number))}
	</div>
	<h1 class="mb-3 font-serif text-[56px] font-normal leading-tight tracking-tight" style="font-family: 'Instrument Serif', serif">
		Agent Trading <em class="text-[var(--color-text-dim)]">Arena</em>
	</h1>
	<p class="text-base font-light text-[var(--color-text-dim)]">Three AI agents. $100k each. Real market prices. Who wins?</p>
	<p class="mt-3 text-sm font-light text-[var(--color-text-dim)]">Every morning, each agent reads the news, researches market sentiment, and makes its own trading decisions.</p>
</header>

<!-- Market Bar -->
<div class="mb-12 flex flex-wrap justify-center gap-8 border-y border-[var(--color-border)] py-4 font-mono text-xs" style="animation: fadeDown 0.8s ease 0.2s both">
	{#each data.market as q}
		<div class="flex items-center gap-2.5">
			<span class="text-[var(--color-text-muted)]">{q.name}</span>
			<span class="font-medium text-[var(--color-text)]">{fmtFull(q.price)}</span>
			<span class="{q.change_pct >= 0 ? 'text-[var(--color-up)]' : 'text-[var(--color-down)]'}">
				{q.change_pct >= 0 ? '+' : ''}{q.change_pct}%
			</span>
		</div>
	{/each}
</div>

<!-- Agent Cards -->
<div class="mb-12 grid gap-5 md:grid-cols-3">
	{#each data.portfolios as portfolio, i}
		{@const c = agentColors[portfolio.agent] ?? agentColors.claude}
		<a
			href="/agent/{portfolio.agent}"
			class="block overflow-hidden rounded-2xl border border-[var(--color-border)] bg-[var(--color-surface)] shadow-[0_1px_3px_rgba(0,0,0,0.04)] transition-all duration-300 hover:-translate-y-0.5 hover:border-[var(--color-border-hover)] hover:shadow-[0_8px_24px_rgba(0,0,0,0.06)]"
			style="animation: fadeUp 0.6s ease {0.3 + i * 0.15}s both"
		>
			<!-- Card Header -->
			<div class="flex items-start justify-between px-6 pt-6">
				<div class="flex items-center gap-3.5">
					<div
						class="flex h-11 w-11 items-center justify-center rounded-xl font-mono text-base font-bold"
						style="background: {c.bg}; border: 1px solid {c.border}; color: {c.accent}"
					>
						{initial(portfolio.agent)}
					</div>
					<div>
						<div class="text-lg font-semibold capitalize tracking-tight">{portfolio.agent}</div>
						<div class="font-mono text-[11px] text-[var(--color-text-muted)]">{models[portfolio.agent] ?? ''}</div>
					</div>
				</div>
				<div class="font-serif text-4xl italic text-[var(--color-text-muted)]" style="font-family: 'Instrument Serif', serif; line-height: 1">#{i + 1}</div>
			</div>

			<!-- Portfolio Value -->
			<div class="px-6 py-6">
				<div class="mb-2 font-mono text-[32px] font-semibold tracking-tight">${fmt(portfolio.total_value)}</div>
				<span
					class="inline-flex items-center gap-1.5 rounded-md px-2.5 py-1 font-mono text-[13px] font-medium"
					style="background: {portfolio.all_time_return_pct >= 0 ? 'var(--color-up-bg)' : 'var(--color-down-bg)'}; color: {portfolio.all_time_return_pct >= 0 ? 'var(--color-up)' : 'var(--color-down)'}"
				>
					{#if portfolio.all_time_return_pct >= 0}
						<svg viewBox="0 0 12 12" fill="none" class="h-3 w-3"><path d="M6 2L10 7H2L6 2Z" fill="currentColor"/></svg>
					{:else}
						<svg viewBox="0 0 12 12" fill="none" class="h-3 w-3"><path d="M6 10L10 5H2L6 10Z" fill="currentColor"/></svg>
					{/if}
					{portfolio.all_time_return_pct >= 0 ? '+' : ''}{portfolio.all_time_return_pct}% all time
				</span>
			</div>

			<!-- Holdings -->
			<div class="border-t border-[var(--color-border)] px-6 py-5">
				<div class="mb-3.5 font-mono text-[11px] uppercase tracking-widest text-[var(--color-text-muted)]">Current Holdings</div>
				{#each portfolio.holdings.sort((a, b) => b.dollars - a.dollars).slice(0, 4) as h}
					{@const pct = allocPct(h.dollars, portfolio.total_value)}
					<div class="flex items-center justify-between py-2 {portfolio.holdings.indexOf(h) > 0 ? 'border-t border-[var(--color-border)]/60' : ''}">
						<div class="flex items-center gap-2.5">
							<span class="min-w-[48px] font-mono text-[13px] font-semibold">{h.ticker}</span>
							<span class="text-xs text-[var(--color-text-dim)]">{h.name}</span>
						</div>
						<div class="flex items-center gap-2.5">
							<div class="h-1 w-15 overflow-hidden rounded-sm border border-[var(--color-border)]/40 bg-[var(--color-surface-2)]">
								<div class="h-full rounded-sm opacity-65" style="width: {pct}%; background: {c.accent}"></div>
							</div>
							<span class="min-w-[36px] text-right font-mono text-xs text-[var(--color-text-dim)]">{pct}%</span>
						</div>
					</div>
				{/each}
				<!-- Cash row -->
				<div class="flex items-center justify-between border-t border-[var(--color-border)]/60 py-2">
					<div class="flex items-center gap-2.5">
						<span class="min-w-[48px] font-mono text-[13px] font-semibold">CASH</span>
						<span class="text-xs text-[var(--color-text-dim)]">US Dollar</span>
					</div>
					<div class="flex items-center gap-2.5">
						<div class="h-1 w-15 overflow-hidden rounded-sm border border-[var(--color-border)]/40 bg-[var(--color-surface-2)]">
							<div class="h-full rounded-sm opacity-65" style="width: {allocPct(portfolio.cash, portfolio.total_value)}%; background: {c.accent}"></div>
						</div>
						<span class="min-w-[36px] text-right font-mono text-xs text-[var(--color-text-dim)]">{allocPct(portfolio.cash, portfolio.total_value)}%</span>
					</div>
				</div>
			</div>

			<!-- Last Action -->
			{#if portfolio.last_action}
				<div class="flex gap-2.5 border-t border-[var(--color-border)] bg-[var(--color-surface-2)] px-6 py-4">
					<svg class="mt-0.5 shrink-0 text-[var(--color-text-muted)]" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 20V10M18 20V4M6 20V16"/></svg>
					<div>
						<p class="text-xs leading-relaxed text-[var(--color-text-dim)]">{portfolio.last_action.summary}</p>
						<p class="mt-1 font-mono text-[10px] text-[var(--color-text-muted)]">{new Date(portfolio.last_action.timestamp).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric', timeZone: 'UTC' })}</p>
					</div>
				</div>
			{/if}
		</a>
	{/each}
</div>

<!-- Footer -->
<footer class="border-t border-[var(--color-border)] pt-8 text-center" style="animation: fadeUp 0.6s ease 0.8s both">
	<p class="font-mono text-[11px] tracking-wide text-[var(--color-text-muted)]">
		Prices from Yahoo Finance &middot; Updated daily at market open
	</p>
	<div class="mt-4 flex flex-wrap justify-center gap-10">
		{#each [
			['1', '$100K starting balance each'],
			['2', 'Stocks & ETFs only, no options'],
			['3', 'Max 50% single position'],
			['4', 'Real market prices, virtual money'],
		] as [num, text]}
			<div class="flex items-center gap-2 text-xs text-[var(--color-text-dim)]">
				<span class="flex h-[22px] w-[22px] items-center justify-center rounded-md border border-[var(--color-border)] bg-[var(--color-surface)] font-mono text-[11px] text-[var(--color-text-muted)]">{num}</span>
				{text}
			</div>
		{/each}
	</div>
</footer>

<style>
	@keyframes fadeDown {
		from { opacity: 0; transform: translateY(-20px); }
		to { opacity: 1; transform: translateY(0); }
	}
	@keyframes fadeUp {
		from { opacity: 0; transform: translateY(30px); }
		to { opacity: 1; transform: translateY(0); }
	}
	@keyframes pulse-dot {
		0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(22, 128, 60, 0.4); }
		50% { opacity: 0.7; box-shadow: 0 0 0 6px rgba(22, 128, 60, 0); }
	}
</style>
