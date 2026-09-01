import { fetchPortfolios, fetchHistory, fetchDiary, fetchMemory, refreshPortfolioPrices } from '$lib/server/boxes.js';
import { error } from '@sveltejs/kit';
import type { PageServerLoad } from './$types.js';

export const load: PageServerLoad = async ({ params, url }) => {
	const name = params.name;
	const test = url.searchParams.has('test');

	const [portfolios, history, diary, memory] = await Promise.all([
		fetchPortfolios(test),
		fetchHistory(name, test),
		fetchDiary(name, test),
		fetchMemory(name, test)
	]);

	const rawPortfolio = portfolios.find((p) => p.agent === name);
	if (!rawPortfolio) throw error(404, `Agent "${name}" not found`);
	const portfolio = await refreshPortfolioPrices(rawPortfolio);

	// Collect all trades from history
	console.log(`[${name}] history snapshots: ${history.length}, trades per snapshot:`, history.map(s => ({ date: s.date, trades: s.trades?.length ?? 0, keys: Object.keys(s) })));
	const allTrades = history
		.flatMap((s) =>
			(s.trades ?? []).map((t) => ({
				...t,
				date: s.date,
				day_number: s.day_number
			}))
		)
		.reverse();

	return { portfolio, history, diary, memory, trades: allTrades };
};
