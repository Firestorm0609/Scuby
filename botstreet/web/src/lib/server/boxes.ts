import { getBoxByName } from '$lib/server/box.js';
import { getOrSetCache } from '$lib/server/cache.js';
import type { Portfolio, HistorySnapshot, MarketQuote } from '$lib/types.js';

const PROD_AGENTS = [
	{ name: 'claude', boxName: 'botstreet-claude-v10' },
	{ name: 'gemini', boxName: 'botstreet-gemini-v10' },
	{ name: 'openai', boxName: 'botstreet-openai-v10' }
] as const;

const TEST_AGENTS = [
	{ name: 'claude', boxName: 'test-claude-v2' },
	{ name: 'gemini', boxName: 'test-gemini-v2' },
	{ name: 'openai', boxName: 'test-openai-v2' }
] as const;

function getAgents(test = false) {
	return test ? TEST_AGENTS : PROD_AGENTS;
}

const HOMEPAGE_CACHE_TTL_MS = 30 * 1000;
const BOX_ROOT = '/workspace/home';
const DATA_DIR = `${BOX_ROOT}/data`;

async function readJson<T>(box: Awaited<ReturnType<typeof getBoxByName>>, path: string): Promise<T | null> {
	try {
		const raw = await box.files.read(path);
		return JSON.parse(raw) as T;
	} catch {
		return null;
	}
}

export async function fetchPortfolios(test = false): Promise<Portfolio[]> {
	const portfolios = await Promise.all(
		getAgents(test).map(async (agent) => {
			try {
				const box = await getBoxByName(agent.boxName);
				return await readJson<Portfolio>(box, `${DATA_DIR}/portfolio.json`);
			} catch (e) {
				console.error(`[fetchPortfolios] ${agent.name} failed:`, e);
				return null;
			}
		})
	);
	return portfolios.filter((p): p is Portfolio => p !== null);
}

async function fetchLivePrice(ticker: string): Promise<number | null> {
	try {
		const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?range=1d&interval=1d`;
		const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
		const data = (await res.json()) as any;
		return data?.chart?.result?.[0]?.meta?.regularMarketPrice ?? null;
	} catch {
		return null;
	}
}

export async function refreshPortfolioPrices(portfolio: Portfolio): Promise<Portfolio> {
	if (portfolio.holdings.length === 0) return portfolio;

	const tickers = portfolio.holdings.map((h) => h.ticker);
	const prices = await Promise.all(tickers.map(fetchLivePrice));
	const priceMap = new Map(tickers.map((t, i) => [t, prices[i]]));

	const updatedHoldings = portfolio.holdings.map((h) => {
		const livePrice = priceMap.get(h.ticker);
		if (!livePrice || livePrice <= 0) return h;
		const dollars = Math.round(h.shares * livePrice * 100) / 100;
		const unrealized_pnl_pct = Math.round(((livePrice - h.avg_entry_price) / h.avg_entry_price) * 10000) / 100;
		return { ...h, current_price: livePrice, dollars, unrealized_pnl_pct };
	});

	const totalValue = Math.round((portfolio.cash + updatedHoldings.reduce((s, h) => s + h.dollars, 0)) * 100) / 100;
	const allTimeReturn = Math.round(((totalValue - portfolio.starting_balance) / portfolio.starting_balance) * 10000) / 100;

	return {
		...portfolio,
		holdings: updatedHoldings,
		total_value: totalValue,
		all_time_return_pct: allTimeReturn,
	};
}

export async function fetchHistory(agentName: string, test = false): Promise<HistorySnapshot[]> {
	const agent = getAgents(test).find((a) => a.name === agentName);
	if (!agent) return [];

	try {
		const box = await getBoxByName(agent.boxName);
		const files = await box.files.list(`${DATA_DIR}/history`);
		const jsonFiles = files
			.filter((f: any) => f.name?.endsWith('.json') || f.path?.endsWith('.json'))
			.sort((a: any, b: any) => (a.path ?? a.name).localeCompare(b.path ?? b.name));

		const snapshots = await Promise.all(
			jsonFiles.map((f: any) => readJson<HistorySnapshot>(box, f.path))
		);
		return snapshots.filter((s): s is HistorySnapshot => s !== null);
	} catch {
		return [];
	}
}

export async function fetchDiary(agentName: string, test = false): Promise<string> {
	const agent = getAgents(test).find((a) => a.name === agentName);
	if (!agent) return '';

	try {
		const box = await getBoxByName(agent.boxName);
		return (await box.files.read(`${DATA_DIR}/diary.md`)) ?? '';
	} catch {
		return '';
	}
}

export async function fetchMemory(agentName: string, test = false): Promise<string> {
	const agent = getAgents(test).find((a) => a.name === agentName);
	if (!agent) return '';

	try {
		const box = await getBoxByName(agent.boxName);
		return (await box.files.read(`${DATA_DIR}/memory.md`)) ?? '';
	} catch {
		return '';
	}
}

export async function fetchMarketData(): Promise<MarketQuote[]> {
	const tickers = [
		{ ticker: '^GSPC', name: 'S&P 500' },
		{ ticker: '^IXIC', name: 'NASDAQ' },
		{ ticker: '^DJI', name: 'DOW' },
		{ ticker: '^VIX', name: 'VIX' }
	];

	const quotes = await Promise.all(
		tickers.map(async ({ ticker, name }) => {
			try {
				const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(ticker)}?range=1d&interval=1d`;
				const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
				const data = (await res.json()) as any;
				const meta = data?.chart?.result?.[0]?.meta;
				if (!meta) return { ticker, name, price: 0, change_pct: 0 };
				const prev = meta.chartPreviousClose ?? meta.regularMarketPrice;
				const change_pct = prev ? ((meta.regularMarketPrice - prev) / prev) * 100 : 0;
				return { ticker, name, price: meta.regularMarketPrice, change_pct: Math.round(change_pct * 100) / 100 };
			} catch {
				return { ticker, name, price: 0, change_pct: 0 };
			}
		})
	);
	return quotes;
}

export async function fetchHomepageData(test = false): Promise<{ portfolios: Portfolio[]; market: MarketQuote[] }> {
	const cacheKey = test ? 'homepage:data:test' : 'homepage:data';
	return getOrSetCache(cacheKey, HOMEPAGE_CACHE_TTL_MS, async () => {
		const [rawPortfolios, market] = await Promise.all([fetchPortfolios(test), fetchMarketData()]);
		const portfolios = await Promise.all(rawPortfolios.map(refreshPortfolioPrices));
		const sorted = [...portfolios].sort((a, b) => b.total_value - a.total_value);

		return { portfolios: sorted, market };
	});
}
